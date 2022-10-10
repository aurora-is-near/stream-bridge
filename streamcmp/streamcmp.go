package streamcmp

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-bridge/stream"
	"github.com/aurora-is-near/stream-bridge/util"
)

const stdoutInterval = time.Second * 5

type StreamCmp struct {
	Mode                  string
	StreamA               *stream.AutoReader
	StreamB               *stream.AutoReader
	StartSeqA             uint64
	StartSeqB             uint64
	SkipDuplicates        bool
	SkipUnequalDuplicates bool
	SkipGaps              bool
	SkipDownjumps         bool
	SkipCorrupted         bool
	SkipDiscrepancy       bool

	interrupt chan os.Signal

	lastA      util.AtomicPtr[streamMsg]
	lastB      util.AtomicPtr[streamMsg]
	stdoutWg   sync.WaitGroup
	stdoutStop chan struct{}
}

func (sc *StreamCmp) Run() error {
	sc.interrupt = make(chan os.Signal, 10)
	signal.Notify(sc.interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT, syscall.SIGUSR1)

	sc.stdoutStop = make(chan struct{})
	sc.stdoutWg.Add(1)
	go sc.stdoutLoop()
	defer sc.stdoutWg.Wait()
	defer close(sc.stdoutStop)

	sc.StreamA.Start(sc.StartSeqA)
	defer sc.StreamA.Stop()
	sc.StreamB.Start(sc.StartSeqB)
	defer sc.StreamB.Stop()

	var err error
	var curA, curB *streamMsg

	for {
		var fetchA bool
		var compare bool

		if curA == nil || curB == nil {
			fetchA = (curA == nil)
			compare = false
		} else {
			if sc.Mode == "aurora" || sc.Mode == "near" {
				fetchA = curA.block.Height <= curB.block.Height
				compare = curA.block.Height == curB.block.Height
			} else {
				relSeqA := curA.out.Metadata.Sequence.Stream - sc.StartSeqA
				relSeqB := curB.out.Metadata.Sequence.Stream - sc.StartSeqB
				fetchA = relSeqA <= relSeqB
				compare = relSeqA == relSeqB
			}
		}

		if compare && !bytes.Equal(curA.raw, curB.raw) {
			errMsg := fmt.Sprintf(
				"%v [DISCREPANCY]: aSeq=%v, bSeq=%v",
				time.Now().Format(time.RFC3339),
				curA.out.Metadata.Sequence.Stream,
				curB.out.Metadata.Sequence.Stream,
			)
			if sc.Mode == "aurora" || sc.Mode == "near" {
				errMsg += fmt.Sprintf(", height=%v", curA.block.Height)
			}
			fmt.Println(errMsg)
			if !sc.SkipDiscrepancy {
				return errors.New(errMsg)
			}
		}

		if fetchA {
			curA, err = sc.getNextValid("A", sc.StreamA, curA, &sc.lastA)
		} else {
			curB, err = sc.getNextValid("B", sc.StreamB, curB, &sc.lastB)
		}
		if err == errInterrupted {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (sc *StreamCmp) stdoutLoop() {
	defer sc.stdoutWg.Done()

	ticker := time.NewTicker(stdoutInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sc.stdoutStop:
			return
		default:
		}

		select {
		case <-sc.stdoutStop:
			return
		case <-ticker.C:
			info := []string{}

			a := sc.lastA.Load()
			if a != nil {
				info = append(info, fmt.Sprintf("aSeq=%v", a.out.Metadata.Sequence.Stream))
				if a.block != nil {
					info = append(info, fmt.Sprintf("aHeight=%v", a.block.Height))
				}
			}

			b := sc.lastB.Load()
			if b != nil {
				info = append(info, fmt.Sprintf("bSeq=%v", b.out.Metadata.Sequence.Stream))
				if b.block != nil {
					info = append(info, fmt.Sprintf("bHeight=%v", b.block.Height))
				}
			}

			fmt.Printf("%v [STATE]: %v\n", time.Now().Format(time.RFC3339), strings.Join(info, ", "))
		}
	}
}
