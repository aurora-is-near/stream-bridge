package blockwriter

import (
	"context"
	"errors"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/aurora-is-near/stream-bridge/blockparse"
	"github.com/aurora-is-near/stream-bridge/stream"
	"github.com/aurora-is-near/stream-bridge/types"
	"github.com/aurora-is-near/stream-bridge/util"
	"github.com/nats-io/nats.go"
)

var (
	ErrCorruptedTip = errors.New("tip block is corrupted")
	ErrLowHeight    = errors.New("low height")
	ErrHashMismatch = errors.New("hash mismatch")
	ErrCanceled     = errors.New("canceled")
)

type Opts struct {
	PublishAckWaitMs uint
	MaxWriteAttempts uint
	WriteRetryWaitMs uint
	TipTtlSeconds    float64
}

type BlockWriter struct {
	opts           *Opts
	output         *stream.Stream
	parseBlock     blockparse.ParseBlockFn
	publishAckWait nats.AckWait

	tip     *types.AbstractBlock
	tipErr  error
	tipTime time.Time
	tipMtx  sync.Mutex

	lastWritten util.AtomicPtr[types.AbstractBlock]
}

func (opts Opts) FillMissingFields() *Opts {
	if opts.PublishAckWaitMs == 0 {
		opts.PublishAckWaitMs = 5000
	}
	if opts.MaxWriteAttempts == 0 {
		opts.MaxWriteAttempts = 3
	}
	if opts.TipTtlSeconds < 0.00001 {
		opts.TipTtlSeconds = 15
	}
	return &opts
}

func NewBlockWriter(opts *Opts, output *stream.Stream, parseBlock blockparse.ParseBlockFn) (*BlockWriter, *types.AbstractBlock, error) {
	opts = opts.FillMissingFields()
	bw := &BlockWriter{
		opts:           opts,
		output:         output,
		parseBlock:     parseBlock,
		publishAckWait: nats.AckWait(time.Millisecond * time.Duration(opts.PublishAckWaitMs)),
	}

	info, _, err := output.GetInfo(0)
	if err != nil {
		return nil, nil, err
	}

	if dedup := info.Config.Duplicates.Seconds(); dedup > 0 && bw.opts.TipTtlSeconds > dedup/2 {
		log.Printf("BlockWriter: TipTtlSeconds (%vs) > dedup / 2 (%vs), lowering it down", bw.opts.TipTtlSeconds, dedup/2)
		bw.opts.TipTtlSeconds = dedup / 2
	}
	if maxAge := info.Config.MaxAge.Seconds(); maxAge > 0 && bw.opts.TipTtlSeconds > maxAge/2 {
		log.Printf("BlockWriter: TipTtlSeconds (%vs) > maxAge / 2 (%vs), lowering it down", bw.opts.TipTtlSeconds, maxAge/2)
		bw.opts.TipTtlSeconds = maxAge / 2
	}
	if bw.opts.TipTtlSeconds < 0.5 {
		log.Printf("BlockWriter: TipTtlSeconds is too low, setting it to 0.5")
		bw.opts.TipTtlSeconds = 0.5
	}

	tip, _, err := bw.GetTip(0)
	if err != nil {
		return nil, nil, err
	}

	return bw, tip, nil
}

func (bw *BlockWriter) Write(ctx context.Context, block *types.AbstractBlock, data []byte) (*nats.PubAck, error) {
	tip, _, err := bw.GetTip(time.Duration(bw.opts.TipTtlSeconds) * time.Second)
	if err != nil {
		return nil, err
	}
	if tip != nil {
		if block.Height <= tip.Height {
			return nil, ErrLowHeight
		}
		if block.PrevHash != tip.Hash {
			return nil, ErrHashMismatch
		}
	}

	var lastErr error
	for attempt := 1; attempt <= int(bw.opts.MaxWriteAttempts); attempt++ {
		if attempt > 1 && bw.opts.WriteRetryWaitMs > 0 {
			log.Printf("BlockWriter: waiting for %vms before next write attempt...", bw.opts.WriteRetryWaitMs)
			if !util.CtxSleep(ctx, time.Millisecond*time.Duration(bw.opts.WriteRetryWaitMs)) {
				return nil, ErrCanceled
			}
		}

		if ctx.Err() != nil {
			return nil, ErrCanceled
		}

		header := make(nats.Header)
		header.Add(nats.MsgIdHdr, strconv.FormatUint(block.Height, 10))
		header.Add(nats.ExpectedLastMsgIdHdr, strconv.FormatUint(tip.Height, 10))
		header.Add(nats.ExpectedLastSeqHdr, strconv.FormatUint(tip.Sequence, 10))

		var ack *nats.PubAck
		ack, lastErr = bw.output.Write(data, header, bw.publishAckWait)
		if lastErr == nil {
			bw.lastWritten.Store(&types.AbstractBlock{
				Hash:     block.Hash,
				PrevHash: block.PrevHash,
				Height:   block.Height,
				Sequence: ack.Sequence,
			})
			return ack, nil
		}

		log.Printf("BlockWriter: write attempt [%v / %v] failed: %v", attempt, bw.opts.MaxWriteAttempts, lastErr)
	}

	return nil, lastErr
}

func (bw *BlockWriter) GetTip(ttl time.Duration) (*types.AbstractBlock, time.Time, error) {
	bw.tipMtx.Lock()
	defer bw.tipMtx.Unlock()
	if ttl == 0 || time.Since(bw.tipTime) > time.Duration(ttl) {
		bw.tip, bw.tipTime, bw.tipErr = bw.getTip(ttl)
	}

	lastWritten := bw.lastWritten.Load()
	if lastWritten != nil && bw.tipErr == nil && (bw.tip == nil || bw.tip.Height < lastWritten.Height) {
		return lastWritten, bw.tipTime, nil
	}
	return bw.tip, bw.tipTime, bw.tipErr
}

func (bw *BlockWriter) getTip(ttl time.Duration) (*types.AbstractBlock, time.Time, error) {
	info, infoTime, err := bw.output.GetInfo(ttl)
	if err != nil {
		return nil, infoTime, err
	}

	if info.State.LastSeq == 0 {
		return nil, infoTime, nil
	}

	msg, err := bw.output.Get(info.State.LastSeq)
	if err != nil {
		return nil, infoTime, err
	}

	block, err := bw.parseBlock(msg.Data, msg.Header)
	if err != nil {
		log.Printf("Block writer: corrupted tip (seq=%d): %v", info.State.LastSeq, err)
		return nil, infoTime, ErrCorruptedTip
	}
	block.Sequence = info.State.LastSeq

	return block, infoTime, nil
}
