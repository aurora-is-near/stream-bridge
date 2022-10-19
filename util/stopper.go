package util

import "sync"

type Stopper struct {
	stop chan struct{}
	wg   sync.WaitGroup
}

func NewStopper() *Stopper {
	s := &Stopper{}
	s.stop = make(chan struct{})
	return s
}

func (s *Stopper) Add(n int) {
	s.wg.Add(n)
}

func (s *Stopper) Done() {
	s.wg.Done()
}

func (s *Stopper) Stopped() <-chan struct{} {
	return s.stop
}

func (s *Stopper) IsStopped() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

func (s *Stopper) Stop() {
	close(s.stop)
	s.wg.Wait()
}
