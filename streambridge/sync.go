package streambridge

import (
	"fmt"

	"github.com/aurora-is-near/stream-bridge/transport"
)

type SyncStopReason struct {
	Error       error
	Recoverable bool
}

type Sync struct {
	bridge *StreamBridge

	stop    chan bool
	stopped chan SyncStopReason

	inputNats  *transport.NatsConnection
	outputNats *transport.NatsConnection
}

func StartSync(bridge *StreamBridge) *Sync {
	s := &Sync{
		bridge:  bridge,
		stop:    make(chan bool, 1),
		stopped: make(chan SyncStopReason, 1),
	}

	go func() {
		s.stopped <- s.run()
		close(s.stopped)
	}()

	return s
}

func (s *Sync) Stopped() <-chan SyncStopReason {
	return s.stopped
}

func (s *Sync) Stop() {
	for {
		select {
		case s.stop <- true:
		case <-s.stopped:
			return
		}
	}
}

func (s *Sync) run() SyncStopReason {
	var err error

	s.inputNats, err = transport.ConnectNATS(s.bridge.InputNats, nil)
	if err != nil {
		return SyncStopReason{
			Error:       fmt.Errorf("unable to connect to input NATS: %v", err),
			Recoverable: false,
		}
	}
	defer s.inputNats.Drain()

	s.outputNats, err = transport.ConnectNATS(s.bridge.OutputNats, nil)
	if err != nil {
		return SyncStopReason{
			Error:       fmt.Errorf("unable to connect to output NATS: %v", err),
			Recoverable: false,
		}
	}
	defer s.outputNats.Drain()

	// TODO: Complete
	return SyncStopReason{
		Error:       nil,
		Recoverable: false,
	}
}
