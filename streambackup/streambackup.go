package streambackup

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-backup/chunks"
	"github.com/aurora-is-near/stream-backup/messagebackup"
	"github.com/aurora-is-near/stream-bridge/stream"
	"github.com/aurora-is-near/stream-bridge/types"
)

const stdoutInterval = time.Second * 5

var errInterrupted = errors.New("interrupted")

type StreamBackup struct {
	Mode     string
	Chunks   *chunks.Chunks
	Reader   *stream.AutoReader
	StartSeq uint64
	EndSeq   uint64

	interrupt chan os.Signal
}

func (sb *StreamBackup) Run() error {
	log.Printf("StreamBackup: opening chunks dir...")
	if err := sb.Chunks.Open(); err != nil {
		return err
	}
	defer sb.Chunks.Flush()
	defer sb.Chunks.CloseReader()
	defer log.Printf("StreamBackup: closing chunks...")

	sb.interrupt = make(chan os.Signal, 10)
	signal.Notify(sb.interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT, syscall.SIGUSR1)

	for {
		select {
		case <-sb.interrupt:
			return nil
		default:
		}

		l, r, err := sb.Chunks.GetLeftmostAbsentRange(sb.StartSeq, sb.EndSeq-1)
		if err == chunks.ErrNotFound {
			log.Printf("Finished")
			return nil
		}
		if err != nil {
			return fmt.Errorf("can't figure out next range: %w", err)
		}

		log.Printf("Pulling segment [%d, %d]", l, r)
		err = sb.pullSegment(l, r)
		if err == errInterrupted {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (sb *StreamBackup) pullSegment(l, r uint64) error {
	var prevBlock *types.AbstractBlock
	if (sb.Mode == "aurora" || sb.Mode == "near") && l > sb.StartSeq {
		if err := sb.Chunks.SeekReader(l - 1); err != nil {
			return fmt.Errorf("can't seek to prev block: %w", err)
		}
		prev, prevData, err := sb.Chunks.ReadNext()
		sb.Chunks.CloseReader()
		if err != nil {
			return fmt.Errorf("can't read prev block: %w", err)
		}
		if prev != l-1 {
			return fmt.Errorf("prev != l - 1")
		}
		mb := &messagebackup.MessageBackup{}
		if err := mb.UnmarshalVT(prevData); err != nil {
			return fmt.Errorf("can't unmarshal prev block: %w", err)
		}
		prevBlock, err = sb.decodeBlock(mb.Data)
		if err != nil {
			return fmt.Errorf("can't decode prev block: %v", err)
		}
	}

	sb.Reader.Start(l)
	defer sb.Reader.Stop()

	stdoutTicker := time.NewTicker(stdoutInterval)
	defer stdoutTicker.Stop()

	for {
		if l > r {
			return nil
		}

		select {
		case <-sb.interrupt:
			return errInterrupted
		default:
		}

		select {
		case <-sb.interrupt:
			return errInterrupted
		case <-stdoutTicker.C:
			fmt.Printf("%v [STATE] curSeq=%v\n", time.Now().Format(time.RFC3339), l)
		case cur := <-sb.Reader.Output():
			block, err := sb.decodeBlock(cur.Msg.Data)
			if err != nil {
				return fmt.Errorf("can't decode new block on seq %v: %w", cur.Metadata.Sequence.Stream, err)
			}
			if prevBlock != nil && prevBlock.Hash != block.PrevHash {
				return fmt.Errorf("hash mismatch on seq %v", cur.Metadata.Sequence.Stream)
			}
			mb := &messagebackup.MessageBackup{
				Headers:  make(map[string]*messagebackup.HeaderValues),
				UnixNano: uint64(cur.Metadata.Timestamp.UnixNano()),
				Data:     cur.Msg.Data,
				Sequence: cur.Metadata.Sequence.Stream,
			}
			for header, values := range cur.Msg.Header {
				mb.Headers[header] = &messagebackup.HeaderValues{Values: values}
			}
			data, err := mb.MarshalVT()
			if err != nil {
				return fmt.Errorf("can't marshal new block on seq %v: %w", cur.Metadata.Sequence.Stream, err)
			}
			if err := sb.Chunks.Write(l, data); err != nil {
				return fmt.Errorf("can't write new block on seq %v: %w", cur.Metadata.Sequence.Stream, err)
			}
			prevBlock = block
			l++
		}
	}
}

func (sb *StreamBackup) decodeBlock(data []byte) (*types.AbstractBlock, error) {
	if sb.Mode == "aurora" {
		block, err := types.DecodeAuroraBlock(data)
		if err != nil {
			return nil, err
		}
		return block.ToAbstractBlock(), err
	}
	if sb.Mode == "near" {
		block, err := types.DecodeNearBlock(data)
		if err != nil {
			return nil, err
		}
		return block.ToAbstractBlock(), err
	}
	return &types.AbstractBlock{}, nil
}
