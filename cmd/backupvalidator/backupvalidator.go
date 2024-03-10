package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-backup/chunks"
	"github.com/aurora-is-near/stream-backup/messagebackup"
	"github.com/aurora-is-near/stream-bridge/blockparse"
	"github.com/aurora-is-near/stream-bridge/types"
	"github.com/nats-io/nats.go"
)

var (
	backupPath    = flag.String("backup", "", "Path to backup (dir + prefix)")
	seqStart      = flag.Uint64("seq-start", 1, "start sequence on stream")
	seqEnd        = flag.Uint64("seq-end", 99999999999, "end sequence on stream")
	mode          = flag.String("mode", "near", "must be one of ['aurora', 'near']")
	ignoreSeqGaps = flag.Bool("ignore-seq-gaps", false, "ignore sequence gaps")
	ignoreMsgId   = flag.Bool("ignore-msgid", false, "ignore msgid")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "finished with error: %v", err)
		os.Exit(1)
	}
}

type Msg struct {
	Seq              uint64
	BackupData       []byte
	DecodingFinished chan struct{}
	DecodingError    error
	Block            *types.AbstractBlock
}

var (
	parseBlockFn blockparse.ParseBlockFn

	stopCh = make(chan struct{})
	errCh  = make(chan error, 100)
	wg     sync.WaitGroup

	decodingQueue   = make(chan *Msg, 1000)
	processingQueue = make(chan *Msg, 1000)
	finished        = make(chan struct{})
)

func run() error {
	var err error
	if parseBlockFn, err = blockparse.GetParseBlockFn(*mode); err != nil {
		return fmt.Errorf("unable to get block parse function for mode '%s': %w", *mode, err)
	}

	defer wg.Wait()
	defer log.Printf("Waiting for all routines to finish...")
	defer close(stopCh)

	startRoutine("reader", runReader)

	numDecoders := runtime.GOMAXPROCS(0)
	for i := 1; i <= numDecoders; i++ {
		startRoutine(fmt.Sprintf("decoder #%d", i), runDecoder)
	}

	startRoutine("processor", runProcessor)

	interrupt := make(chan os.Signal, 10)
	signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT)

	select {
	case <-finished:
		log.Printf("Successfully finished")
		return nil
	case <-interrupt:
		log.Printf("Interrupted, stopping...")
		return nil
	case err := <-errCh:
		log.Printf("Stopping because of error: %v", err)
		return err
	}
}

func startRoutine(name string, fn func() error) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := fn(); err != nil {
			errCh <- fmt.Errorf("routine '%s' finished with error: %w", name, err)
		}
	}()
}

func runReader() error {
	backupChunks := &chunks.Chunks{
		Dir:             filepath.Dir(*backupPath),
		ChunkNamePrefix: filepath.Base(*backupPath),
	}
	if err := backupChunks.Open(); err != nil {
		return fmt.Errorf("unable to open backup: %w", err)
	}
	defer backupChunks.CloseReader()

	log.Printf("Got %d chunks", len(backupChunks.GetChunkRanges()))

	log.Printf("Seeking seq=%d...", *seqStart)
	if err := backupChunks.SeekReader(*seqStart); err != nil {
		return fmt.Errorf("unable to seek seq=%d: %w", *seqStart, err)
	}

	prevSeq := uint64(0)

	log.Printf("Reading...")
	for {
		select {
		case <-stopCh:
			return nil
		default:
		}

		seq, data, err := backupChunks.ReadNext()
		if err != nil {
			if errors.Is(err, chunks.ErrNotFound) {
				if prevSeq == 0 {
					log.Printf("Reading finished, nothing to read")
				} else {
					log.Printf("Reading finished, nothing to read after seq=%d", prevSeq)
				}
				close(decodingQueue)
				close(processingQueue)
				return nil
			}
			return fmt.Errorf("unable to read next msg after seq=%d: %w", prevSeq, err)
		}

		if !*ignoreSeqGaps && prevSeq > 0 && seq != (prevSeq+1) {
			return fmt.Errorf("got read unexpected seq %d after seq %d", seq, prevSeq)
		}

		if seq >= *seqEnd {
			log.Printf("Reading finished, reached end seq: %d >= %d", seq, *seqEnd)
			close(decodingQueue)
			close(processingQueue)
			return nil
		}

		msg := &Msg{
			Seq:              seq,
			BackupData:       data,
			DecodingFinished: make(chan struct{}),
		}

		select {
		case decodingQueue <- msg:
		case <-stopCh:
			return nil
		}

		select {
		case processingQueue <- msg:
		case <-stopCh:
			return nil
		}

		prevSeq = seq
	}
}

func runDecoder() error {
	for {
		select {
		case <-stopCh:
			return nil
		default:
		}

		select {
		case msg, ok := <-decodingQueue:
			if !ok {
				return nil
			}
			msg.DecodingError = decodeMessage(msg)
			close(msg.DecodingFinished)
		case <-stopCh:
			return nil
		}
	}
}

func decodeMessage(msg *Msg) error {
	bkp := &messagebackup.MessageBackup{}
	if err := bkp.UnmarshalVT(msg.BackupData); err != nil {
		return fmt.Errorf("unable to unmarshal message backup: %w", err)
	}

	var err error
	if msg.Block, err = parseBlockFn(bkp.Data, nil); err != nil {
		return fmt.Errorf("unable to parse block: %w", err)
	}

	if *ignoreMsgId {
		return nil
	}

	msgIdHdr, ok := bkp.Headers[nats.MsgIdHdr]
	if !ok {
		return fmt.Errorf("missing '%s' header", nats.MsgIdHdr)
	}
	if len(msgIdHdr.Values) != 1 {
		return fmt.Errorf("header '%s' has %d values, expected exactly 1", nats.MsgIdHdr, len(msgIdHdr.Values))
	}

	msgIdHeight, err := strconv.ParseUint(msgIdHdr.Values[0], 10, 64)
	if err != nil {
		return fmt.Errorf("unable to parse msgid '%s': %w", msgIdHdr.Values[0], err)
	}

	if msgIdHeight != msg.Block.Height {
		return fmt.Errorf("wrong msgid=%d, expected %d", msgIdHeight, msg.Block.Height)
	}

	return nil
}

func runProcessor() error {
	var prevMsg *Msg

	lastLogTime := time.Now()
	msgsSinceLastLog := 0

	for {
		select {
		case <-stopCh:
			return nil
		default:
		}

		var msg *Msg
		var ok bool

		select {
		case msg, ok = <-processingQueue:
			if !ok {
				close(finished)
				return nil
			}
		case <-stopCh:
			return nil
		}

		select {
		case <-msg.DecodingFinished:
		case <-stopCh:
			return nil
		}

		if msg.DecodingError != nil {
			return fmt.Errorf("unable to decode msg on seq=%d: %w", msg.Seq, msg.DecodingError)
		}

		if prevMsg != nil {
			if msg.Block.PrevHash != prevMsg.Block.Hash {
				return fmt.Errorf(
					"msg(seq=%d).PrevHash != msg(prevSeq=%d).Hash: '%s' != '%s'",
					msg.Seq, prevMsg.Seq, msg.Block.PrevHash, prevMsg.Block.Hash,
				)
			}
			if msg.Block.Height <= prevMsg.Block.Height {
				return fmt.Errorf(
					"msg(seq=%d).Height <= msg(prevSeq=%d).Height: %d <= %d",
					msg.Seq, prevMsg.Seq, msg.Block.Height, prevMsg.Block.Height,
				)
			}
		}

		prevMsg = msg
		msgsSinceLastLog++

		now := time.Now()
		tDiff := now.Sub(lastLogTime)
		if tDiff >= time.Second*3 {
			log.Printf(
				"seq=%d, height=%d, speed=%0.2fmsgs/sec",
				msg.Seq,
				msg.Block.Height,
				float64(msgsSinceLastLog)/tDiff.Seconds(),
			)
			lastLogTime = now
			msgsSinceLastLog = 0
		}
	}
}
