package streamcmp

import (
	"log"
	"sync"
	"time"

	"github.com/aurora-is-near/stream-bridge/stream"
)

type StreamWrapper struct {
	Stream          *stream.Opts
	Reader          *stream.ReaderOpts
	StartSeq        uint64
	ReconnectWaitMs uint

	output chan *stream.ReaderOutput
	stop   chan struct{}
	wg     sync.WaitGroup
}

func (sw *StreamWrapper) Start() {
	sw.output = make(chan *stream.ReaderOutput)
	sw.stop = make(chan struct{})
	sw.wg.Add(1)
	go sw.run()
}

func (sc *StreamWrapper) Output() chan *stream.ReaderOutput {
	return sc.output
}

func (sw *StreamWrapper) Stop() {
	close(sw.stop)
	sw.wg.Wait()
}

func (sw *StreamWrapper) run() {
	defer sw.wg.Done()

	nextSeq := sw.StartSeq

	var err error
	var s *stream.Stream
	var r *stream.Reader

	disconnect := func() {
		if r != nil {
			r.Stop()
			r = nil
		}
		if s != nil {
			s.Disconnect()
			s = nil
		}
	}
	defer disconnect()

	connectionProblem := false
	for {
		select {
		case <-sw.stop:
			return
		default:
		}

		if connectionProblem {
			disconnect()
			log.Printf("Waiting for %vms before reconnection...", sw.ReconnectWaitMs)
			timer := time.NewTimer(time.Millisecond * time.Duration(sw.ReconnectWaitMs))
			select {
			case <-sw.stop:
				timer.Stop()
				return
			case <-timer.C:
			}
			connectionProblem = false
		}

		if s == nil || r == nil {
			disconnect()
			s, err = stream.ConnectStream(sw.Stream)
			if err != nil {
				log.Printf("Can't connect stream: %v", err)
				connectionProblem = true
				continue
			}

			select {
			case <-sw.stop:
				return
			default:
			}

			r, err = stream.StartReader(sw.Reader, s, nextSeq, 0)
			if err != nil {
				log.Printf("Can't start reader: %v", err)
				connectionProblem = true
				continue
			}
		}

		select {
		case <-sw.stop:
			return
		default:
		}

		select {
		case <-sw.stop:
			return
		case out, ok := <-r.Output():
			if !ok {
				log.Printf("Reader was stopped for some reason")
				connectionProblem = true
				continue
			}
			if out.Error != nil {
				log.Printf("Reader error: %v", out.Error)
				connectionProblem = true
				continue
			}
			if out.Metadata.Sequence.Stream != nextSeq {
				log.Printf("Got wrong seq from reader. Expected: %v, found: %v", nextSeq, out.Metadata.Sequence.Stream)
				connectionProblem = true
				continue
			}
			select {
			case <-sw.stop:
				return
			case sw.output <- out:
			}
			nextSeq++
		}
	}
}
