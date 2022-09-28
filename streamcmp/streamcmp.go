package streamcmp

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-bridge/stream"
	"github.com/aurora-is-near/stream-bridge/types"
	"github.com/fxamacker/cbor/v2"
)

const stdoutInterval = time.Second * 5

type StreamCmp struct {
	Mode    string
	StreamA *StreamWrapper
	StreamB *StreamWrapper
}

func (sc *StreamCmp) Run() error {
	interrupt := make(chan os.Signal, 10)
	signal.Notify(interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT, syscall.SIGUSR1)

	sc.StreamA.Start()
	defer sc.StreamA.Stop()
	sc.StreamB.Start()
	defer sc.StreamB.Stop()

	lastStdoutTime := time.Now()

	var prevBlock *types.AbstractBlock

	for {
		select {
		case <-interrupt:
			return nil
		default:
		}

		var a, b *stream.ReaderOutput

		select {
		case <-interrupt:
			return nil
		case a = <-sc.StreamA.Output():
		}

		select {
		case <-interrupt:
			return nil
		default:
		}

		select {
		case <-interrupt:
			return nil
		case b = <-sc.StreamB.Output():
		}

		aRaw, err := types.DecodeBorealisPayload[cbor.RawMessage](a.Msg.Data)
		if err != nil {
			return fmt.Errorf("can't decode stream-a borealis-msg on seq %v: %v", a.Metadata.Sequence.Stream, err)
		}

		bRaw, err := types.DecodeBorealisPayload[cbor.RawMessage](b.Msg.Data)
		if err != nil {
			return fmt.Errorf("can't decode stream-b borealis-msg on seq %v: %v", b.Metadata.Sequence.Stream, err)
		}

		if !bytes.Equal(*aRaw, *bRaw) {
			return fmt.Errorf("raw messages are not equal. a-seq: %v, b-seq: %v", a.Metadata.Sequence.Stream, b.Metadata.Sequence.Stream)
		}

		if sc.Mode == "aurora" {
			block, err := types.DecodeAuroraBlock(a.Msg.Data)
			if err != nil {
				return fmt.Errorf("can't decode aurora block. a-seq: %v, b-seq: %v. err: %v", a.Metadata.Sequence.Stream, b.Metadata.Sequence.Stream, err)
			}
			abstractBlock := block.ToAbstractBlock()
			if prevBlock != nil && abstractBlock.Height != prevBlock.Height+1 {
				return fmt.Errorf(
					"block.height (%v) != prevBlock.height + 1 (%v + 1). a-seq: %v, b-seq: %v",
					abstractBlock.Height,
					prevBlock.Height,
					a.Metadata.Sequence.Stream,
					b.Metadata.Sequence.Stream,
				)
			}
			prevBlock = abstractBlock
		}

		if sc.Mode == "near" {
			block, err := types.DecodeNearBlock(a.Msg.Data)
			if err != nil {
				return fmt.Errorf("can't decode near block. a-seq: %v, b-seq: %v. err: %v", a.Metadata.Sequence.Stream, b.Metadata.Sequence.Stream, err)
			}
			abstractBlock := block.ToAbstractBlock()
			if prevBlock != nil && abstractBlock.PrevHash != prevBlock.Hash {
				return fmt.Errorf(
					"block.prevhash (%v) != prevBlock.hash (%v). a-seq: %v, b-seq: %v",
					abstractBlock.PrevHash,
					prevBlock.Hash,
					a.Metadata.Sequence.Stream,
					b.Metadata.Sequence.Stream,
				)
			}
			prevBlock = abstractBlock
		}

		if time.Since(lastStdoutTime) > stdoutInterval {
			log.Printf("a: %v, b: %v", a.Metadata.Sequence.Stream, b.Metadata.Sequence.Stream)
			lastStdoutTime = time.Now()
		}
	}
}
