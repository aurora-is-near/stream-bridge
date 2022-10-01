package streamcmp

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/aurora-is-near/stream-bridge/stream"
	"github.com/aurora-is-near/stream-bridge/types"
	"github.com/aurora-is-near/stream-bridge/util"
	"github.com/fxamacker/cbor/v2"
)

var errInterrupted = errors.New("interrupted")

type streamMsg struct {
	out   *stream.ReaderOutput
	raw   []byte
	block *types.AbstractBlock
}

func (sm *streamMsg) decode(mode string) error {
	raw, err := types.DecodeBorealisPayload[cbor.RawMessage](sm.out.Msg.Data)
	if err != nil {
		return fmt.Errorf("can't decode borealis envelope: %w", err)
	}
	sm.raw = *raw

	if mode == "aurora" {
		auroraBlock, err := types.DecodeAuroraBlock(sm.out.Msg.Data)
		if err != nil {
			return fmt.Errorf("can't decode aurora block: %w", err)
		}
		sm.block = auroraBlock.ToAbstractBlock()
	}
	if mode == "near" {
		nearBlock, err := types.DecodeNearBlock(sm.out.Msg.Data)
		if err != nil {
			return fmt.Errorf("can't decode near block: %w", err)
		}
		sm.block = nearBlock.ToAbstractBlock()
	}

	return nil
}

func (sc *StreamCmp) readNext(s *StreamWrapper) (*streamMsg, error) {
	select {
	case <-sc.interrupt:
		return nil, errInterrupted
	default:
	}

	msg := &streamMsg{}

	select {
	case <-sc.interrupt:
		return nil, errInterrupted
	case msg.out = <-s.Output():
	}

	return msg, nil
}

func (sc *StreamCmp) getNextValid(streamName string, s *StreamWrapper, prev *streamMsg, lastUpd *util.AtomicPtr[streamMsg]) (*streamMsg, error) {
	var err error
	var cur *streamMsg
	for {
		cur, err = sc.readNext(s)
		if err == errInterrupted {
			return nil, errInterrupted
		}
		if err != nil {
			return nil, fmt.Errorf("can't read next message from stream %v: %w", streamName, err)
		}

		err = cur.decode(sc.Mode)
		lastUpd.Store(cur)
		if err != nil {
			err = fmt.Errorf("%v [%v/CORRUPTED]: %w", time.Now().Format(time.RFC3339), streamName, err)
			fmt.Println(err)
			if sc.SkipCorrupted {
				continue
			}
			return nil, err
		}

		break
	}

	if prev == nil {
		return cur, nil
	}

	makeErr := func(errTag string, skippable bool) error {
		err = fmt.Errorf(
			"%v [%v/%v]: prevSeq=%v, prevHeight=%v, curSeq=%v, curHeight=%v",
			time.Now().Format(time.RFC3339),
			streamName,
			errTag,
			prev.out.Metadata.Sequence.Stream,
			prev.block.Height,
			cur.out.Metadata.Sequence.Stream,
			cur.block.Height,
		)
		fmt.Println(err)
		if skippable {
			return nil
		}
		return err
	}

	var sequenceErr error = nil

	if (sc.Mode == "aurora" || sc.Mode == "near") && cur.block.Height < prev.block.Height {
		sequenceErr = makeErr("DOWNJUMP", sc.SkipDownjumps)
	}

	if (sc.Mode == "aurora" || sc.Mode == "near") && cur.block.Height == prev.block.Height {
		if !bytes.Equal(cur.raw, prev.raw) {
			sequenceErr = makeErr("UNEQUALDUPLICATE", sc.SkipUnequalDuplicates && sc.SkipDuplicates)
		} else {
			sequenceErr = makeErr("DUPLICATE", sc.SkipDuplicates)
		}
	}

	isGap := (sc.Mode == "aurora" && cur.block.Height > prev.block.Height+1)
	isGap = isGap || (sc.Mode == "near" && cur.block.Height > prev.block.Height+1 && cur.block.PrevHash != prev.block.Hash)
	if isGap {
		sequenceErr = makeErr("GAP", sc.SkipGaps)
	}

	if sequenceErr != nil {
		return nil, sequenceErr
	}
	return cur, nil
}
