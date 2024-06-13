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
	"sync"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-backup/chunks"
	"github.com/aurora-is-near/stream-backup/messagebackup"
	"github.com/aurora-is-near/stream-bridge/types"
)

var (
	backupPath      = flag.String("backup", "", "path to backup (dir + prefix)")
	outputDir       = flag.String("output", "", "path to state")
	dumpThresholdMb = flag.Int("dump-threshold", 5, "dump threshold (megabytes)")
	seqStart        = flag.Uint64("seq-start", 1, "start sequence on stream")
	seqEnd          = flag.Uint64("seq-end", 99999999999, "end sequence on stream")
	ignoreSeqGaps   = flag.Bool("ignore-seq-gaps", false, "ignore sequence gaps")
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
	BlockV2          []byte
	BlockJson        []byte
}

var (
	curState *State

	stopCh = make(chan struct{})
	errCh  = make(chan error, 100)
	wg     sync.WaitGroup

	decodingQueue   = make(chan *Msg, 1000)
	processingQueue = make(chan *Msg, 1000)
	finished        = make(chan struct{})
)

func run() error {
	if *backupPath == "" {
		return fmt.Errorf("-backup flag must be provided")
	}
	if *outputDir == "" {
		return fmt.Errorf("-output flag must be provided")
	}
	if err := os.MkdirAll(filepath.Join(*outputDir, "dumps"), 0755); err != nil {
		return fmt.Errorf("unable to create directory for dumps: %w", err)
	}

	var err error
	if curState, err = ReadStateOrEmpty(filepath.Join(*outputDir, "state.json")); err != nil {
		return fmt.Errorf("unable to read state: %w", err)
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
		log.Printf("Successfully finished, waiting for interruption...")
		<-interrupt
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

	seekSeq := curState.LastProcessedSeq + 1
	if seekSeq < *seqStart {
		seekSeq = *seqStart
	}

	log.Printf("Seeking seq=%d...", seekSeq)
	if err := backupChunks.SeekReader(seekSeq); err != nil {
		return fmt.Errorf("unable to seek seq=%d: %w", seekSeq, err)
	}

	prevSeq := seekSeq - 1

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
				if prevSeq < seekSeq {
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

		if !*ignoreSeqGaps && prevSeq >= *seqStart && seq != (prevSeq+1) {
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

	msg.BlockV2 = bkp.Data

	var err error
	if msg.BlockJson, err = types.DecodeNearBlockJson(msg.BlockV2); err != nil {
		return fmt.Errorf("unable to decode V2: %w", err)
	}

	if len(msg.BlockJson) > *dumpThresholdMb*1024*1024 {
		path := filepath.Join(*outputDir, "dumps", fmt.Sprintf("%d.json", msg.Seq))
		if err := WriteFileAtomically(path, msg.BlockJson); err != nil {
			return fmt.Errorf("unable to dump file to path '%s': %w", path, err)
		}
	}

	return nil
}

func runProcessor() error {
	lastLogTime := time.Now()
	msgsSinceLastLog := 0
	msgsHandled := uint64(0)

	defer func() {
		log.Printf("Processor: saving state before exit...")
		if err := curState.Save(filepath.Join(*outputDir, "state.json")); err != nil {
			log.Printf("Processor ERROR: unable to save state: %v", err)
		}
	}()

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

		curState.AcknowledgeSize(len(msg.BlockV2), len(msg.BlockJson))
		curState.LastProcessedSeq = msg.Seq

		msgsHandled++
		msgsSinceLastLog++

		if msgsHandled%10000 == 0 {
			if err := curState.Save(filepath.Join(*outputDir, "state.json")); err != nil {
				return fmt.Errorf("unable to save state: %w", err)
			}
		}

		now := time.Now()
		tDiff := now.Sub(lastLogTime)
		if tDiff >= time.Second*3 {
			log.Printf(
				"seq=%d, speed=%0.2fmsgs/sec",
				msg.Seq,
				float64(msgsSinceLastLog)/tDiff.Seconds(),
			)
			lastLogTime = now
			msgsSinceLastLog = 0
		}
	}
}
