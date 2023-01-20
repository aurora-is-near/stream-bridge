package stream

import (
	"log"
	"sync"
	"time"
)

type AutoReader struct {
	Stream          *Opts
	Reader          *ReaderOpts
	ReconnectWaitMs uint

	output chan *ReaderOutput
	stop   chan struct{}
	wg     sync.WaitGroup
}

func (sw *AutoReader) Start(startSeq uint64) {
	sw.output = make(chan *ReaderOutput)
	sw.stop = make(chan struct{})
	sw.wg.Add(1)
	go sw.run(startSeq)
}

func (sc *AutoReader) Output() chan *ReaderOutput {
	return sc.output
}

func (sw *AutoReader) Stop() {
	close(sw.stop)
	sw.wg.Wait()
}

func (sw *AutoReader) run(nextSeq uint64) {
	defer sw.wg.Done()

	var err error
	var s StreamInterface
	var r *Reader

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
			s, err = ConnectStream(sw.Stream)
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

			r, err = StartReader(sw.Reader, s, nextSeq, 0)
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
