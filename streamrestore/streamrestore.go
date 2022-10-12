package streamrestore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-bridge/blockparse"
	"github.com/aurora-is-near/stream-bridge/blockwriter"
	"github.com/aurora-is-near/stream-bridge/stream"
	"github.com/aurora-is-near/stream-bridge/streambackup/chunks"
	"github.com/aurora-is-near/stream-bridge/types"
)

const stdoutInterval = time.Second * 5

var errConnectionProblem = errors.New("connection problem")
var errInterrupted = errors.New("interrupted")
var errStartNotFound = errors.New("start not found")

type StreamRestore struct {
	Mode            string
	Chunks          *chunks.Chunks
	Stream          *stream.Opts
	Writer          *blockwriter.Opts
	StartSeq        uint64
	EndSeq          uint64
	ToleranceWindow uint
	ReconnectWaitMs uint

	parseBlock blockparse.ParseBlockFn
	interrupt  chan os.Signal
}

func (sr *StreamRestore) Run() error {
	if sr.StartSeq >= sr.EndSeq {
		return fmt.Errorf("StartSeq must be less than EndSeq")
	}

	var err error
	sr.parseBlock, err = blockparse.GetParseBlockFn(sr.Mode)
	if err != nil {
		return err
	}

	if err := sr.Chunks.Open(); err != nil {
		return err
	}
	defer sr.Chunks.CloseReader()

	sr.interrupt = make(chan os.Signal, 10)
	signal.Notify(sr.interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT, syscall.SIGUSR1)

	for {
		switch err = sr.push(); err {
		case nil:
			log.Printf("Finished")
			return nil
		case errInterrupted:
			log.Printf("Interrupted")
			return nil
		case errConnectionProblem:
			timer := time.NewTimer(time.Duration(sr.ReconnectWaitMs) * time.Millisecond)
			log.Printf("Sleeping for %vms before next reconnection...", sr.ReconnectWaitMs)
			select {
			case <-sr.interrupt:
				timer.Stop()
				log.Printf("Interrupted")
				return nil
			case <-timer.C:
				continue
			}
		default:
			return err
		}
	}
}

func (sr *StreamRestore) push() error {
	if sr.isInterrupted() {
		return errInterrupted
	}

	stream, err := stream.ConnectStream(sr.Stream)
	if err != nil {
		log.Printf("Got connection problem, will reconnect: %v", err)
		return errConnectionProblem
	}

	if sr.isInterrupted() {
		return errInterrupted
	}

	writer, tip, err := blockwriter.NewBlockWriter(sr.Writer, stream, sr.parseBlock)
	if err != nil {
		log.Printf("Got connection problem, will reconnect: %v", err)
		return errConnectionProblem
	}

	seq, err := sr.findStartChunkSeq(tip)
	if err == errStartNotFound {
		log.Printf("Nothing to do")
		return nil
	}
	if err == errInterrupted {
		return errInterrupted
	}
	if err != nil {
		return fmt.Errorf("can't figure start chunk: %w", err)
	}

	output, stopReading := sr.startReading(seq)
	defer stopReading()

	lastReadSeq, lastWrittenSeq := uint64(0), uint64(0)
	lastStdout := time.Now()

	consecutiveWrongBlocks := 0
	lastBlockWasWrong := false
	for {
		if sr.isInterrupted() {
			return errInterrupted
		}

		if lastBlockWasWrong {
			consecutiveWrongBlocks++
			if consecutiveWrongBlocks > int(sr.ToleranceWindow) {
				return fmt.Errorf("tolerance window exceeded")
			}
			lastBlockWasWrong = false
		} else {
			consecutiveWrongBlocks = 0
		}

		if time.Since(lastStdout) > stdoutInterval {
			fmt.Printf("[STATE]: lastReadSeq=%v, lastWrittenSeq=%v\n", lastReadSeq, lastWrittenSeq)
			lastStdout = time.Now()
		}

		select {
		case <-sr.interrupt:
			return errInterrupted
		case out, ok := <-output:
			if !ok {
				return nil
			}
			if out.err != nil {
				return fmt.Errorf("can't read from backup: %w", out.err)
			}
			bb := out.blockBackup
			lastReadSeq = bb.sequence
			switch ack, err := writer.Write(context.Background(), bb.block, bb.messageBackup.Data); err {
			case nil:
				lastWrittenSeq = ack.Sequence
			case blockwriter.ErrHashMismatch:
				fmt.Printf("[WRONGHASH]: seq=%v\n", bb.sequence)
				lastBlockWasWrong = true
			case blockwriter.ErrLowHeight:
			default:
				log.Printf("Got writer problem, will restart connection: %v", err)
				return errConnectionProblem
			}
		}
	}
}

func (sr *StreamRestore) findStartChunkSeq(tip *types.AbstractBlock) (uint64, error) {
	tipHeight := uint64(0)
	if tip != nil {
		tipHeight = tip.Height
	}

	log.Printf("Getting chunks info...")
	chunkRanges := sr.Chunks.GetChunkRanges()
	chunkStarts := []uint64{}
	for _, cr := range chunkRanges {
		if cr.R < sr.StartSeq || cr.L >= sr.EndSeq {
			continue
		}
		chunkStarts = append(chunkStarts, cr.L)
	}
	if len(chunkStarts) == 0 {
		return 0, errStartNotFound
	}
	sort.Slice(chunkStarts, func(i, j int) bool {
		return chunkStarts[i] < chunkStarts[j]
	})

	log.Printf("Binsearching for the chunk to start with...")
	l, r := 0, len(chunkStarts)
	for l+1 < r {
		if sr.isInterrupted() {
			return 0, errInterrupted
		}

		log.Printf("Binsearch state: [%d;%d)", l, r)
		m := (l + r) / 2

		bb, err := sr.readSingle(chunkStarts[m])
		if err != nil {
			return 0, fmt.Errorf("binsearch: can't read first message of chunk which starts on %v: %w", chunkStarts[m], err)
		}
		if bb.sequence != chunkStarts[m] {
			return 0, fmt.Errorf("binsearch: bb.sequence != chunkStarts[m]")
		}
		if bb.block.Height <= tipHeight {
			l = m
		} else {
			r = m
		}
	}

	log.Printf("Will start with chunk which starts on %v seq", chunkStarts[l])
	return chunkStarts[l], nil
}

func (sr *StreamRestore) isInterrupted() bool {
	select {
	case <-sr.interrupt:
		return true
	default:
		return false
	}
}
