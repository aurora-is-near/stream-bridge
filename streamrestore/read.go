package streamrestore

import (
	"fmt"
	"log"
	"sync"

	"github.com/aurora-is-near/stream-bridge/streambackup/chunks"
	"github.com/aurora-is-near/stream-bridge/streambackup/messagebackup"
	"github.com/aurora-is-near/stream-bridge/types"
	"github.com/nats-io/nats.go"
)

type blockBackup struct {
	sequence      uint64
	messageBackup *messagebackup.MessageBackup
	block         *types.AbstractBlock
}

type readingResult struct {
	blockBackup *blockBackup
	err         error
}

func (sr *StreamRestore) startReading(seq uint64) (output <-chan *readingResult, stop func()) {
	out := make(chan *readingResult, 500)
	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(out)

		if err := sr.Chunks.SeekReader(seq); err != nil {
			out <- &readingResult{err: fmt.Errorf("can't seek reader to pos %v: %w", seq, err)}
			return
		}
		for {
			select {
			case <-stopChan:
				return
			default:
			}

			seq, data, err := sr.Chunks.ReadNext()
			if err == chunks.ErrNotFound {
				log.Printf("No more messages found for reading, finishing")
				return
			}
			if err != nil {
				out <- &readingResult{err: fmt.Errorf("can't read next msg from chunks: %w", err)}
				return
			}
			if seq < sr.StartSeq {
				continue
			}
			if seq >= sr.EndSeq {
				return
			}

			bb, err := sr.parseBlockBackup(seq, data)
			if err != nil {
				out <- &readingResult{err: fmt.Errorf("can't parse block backup on seq %v: %w", seq, err)}
				return
			}

			select {
			case <-stopChan:
				return
			case out <- &readingResult{blockBackup: bb, err: nil}:
			}
		}
	}()

	return out, func() {
		close(stopChan)
		wg.Wait()
	}
}

func (sr *StreamRestore) readSingle(pos uint64) (*blockBackup, error) {
	if err := sr.Chunks.SeekReader(pos); err != nil {
		return nil, fmt.Errorf("can't seek reader to pos %v: %w", pos, err)
	}
	seq, data, err := sr.Chunks.ReadNext()
	if err != nil {
		return nil, fmt.Errorf("can't read next msg from chunks: %w", err)
	}
	bb, err := sr.parseBlockBackup(seq, data)
	if err != nil {
		return nil, fmt.Errorf("can't parse block backup (seq=%v): %w", seq, err)
	}
	return bb, nil
}

func (sr *StreamRestore) parseBlockBackup(sequence uint64, data []byte) (*blockBackup, error) {
	bb := &blockBackup{
		sequence:      sequence,
		messageBackup: &messagebackup.MessageBackup{},
	}
	err := bb.messageBackup.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal message backup (seq=%v): %w", sequence, err)
	}
	headers := make(nats.Header)
	for key, values := range bb.messageBackup.Headers {
		headers[key] = values.Values
	}
	bb.block, err = sr.parseBlock(bb.messageBackup.Data, headers)
	if err != nil {
		return nil, fmt.Errorf("can't parse block from message backup (seq=%v): %w", sequence, err)
	}
	return bb, nil
}
