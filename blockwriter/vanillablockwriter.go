package blockwriter

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/aurora-is-near/stream-bridge/blockparse"
	"github.com/aurora-is-near/stream-bridge/stream"
	"github.com/aurora-is-near/stream-bridge/types"
	"github.com/aurora-is-near/stream-bridge/util"
	"github.com/nats-io/nats.go"
)

type VanillaBlockWriter struct {
	opts           *Opts
	output         stream.StreamWrapperInterface
	parseBlock     blockparse.ParseBlockFn
	publishAckWait nats.AckWait

	tip     *types.AbstractBlock
	tipErr  error
	tipTime time.Time
	tipMtx  sync.Mutex

	lastWritten util.AtomicPtr[types.AbstractBlock]
}

var Logging = false

func NewVanillaBlockWriter(opts *Opts, output stream.StreamWrapperInterface, parseBlock blockparse.ParseBlockFn) (BlockWriterInterface, *types.AbstractBlock, error) {
	opts = opts.FillMissingFields()
	bw := &VanillaBlockWriter{
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
		log.Printf("VanillaBlockWriter: TipTtlSeconds (%vs) > dedup / 2 (%vs), lowering it down", bw.opts.TipTtlSeconds, dedup/2)
		bw.opts.TipTtlSeconds = dedup / 2
	}
	if maxAge := info.Config.MaxAge.Seconds(); maxAge > 0 && bw.opts.TipTtlSeconds > maxAge/2 {
		log.Printf("VanillaBlockWriter: TipTtlSeconds (%vs) > maxAge / 2 (%vs), lowering it down", bw.opts.TipTtlSeconds, maxAge/2)
		bw.opts.TipTtlSeconds = maxAge / 2
	}
	if bw.opts.TipTtlSeconds < 0.5 {
		log.Printf("VanillaBlockWriter: TipTtlSeconds is too low, setting it to 0.5")
		bw.opts.TipTtlSeconds = 0.5
	}

	tip, _, err := bw.GetTip(0)
	if err != nil {
		return nil, nil, err
	}

	return bw, tip, nil
}

func (bw *VanillaBlockWriter) Write(ctx context.Context, block *types.AbstractBlock, data []byte) (*nats.PubAck, error) {
	if Logging {
		log.Printf("VanillaBlockWriter: Write: %d", block.Height)
	}
	msg := &nats.Msg{
		Subject: bw.output.GetStream().Opts.Subject,
		Data:    data,
	}
	pubAck, err := bw.output.GetStream().Js.PublishMsg(msg, bw.publishAckWait)
	if Logging {
		log.Printf("VanillaBlockWriter: pubAck: %+v error: %e", pubAck, err)
	}
	return pubAck, err
}

func (bw *VanillaBlockWriter) GetTip(ttl time.Duration) (*types.AbstractBlock, time.Time, error) {
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

func (bw *VanillaBlockWriter) getTip(ttl time.Duration) (*types.AbstractBlock, time.Time, error) {
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
		log.Printf("Vanilla Block writer: corrupted tip (seq=%d): %v", info.State.LastSeq, err)
		return nil, infoTime, ErrCorruptedTip
	}
	block.Sequence = info.State.LastSeq

	return block, infoTime, nil
}
