package stream

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aurora-is-near/stream-bridge/transport"
	"github.com/nats-io/nats.go"
)

type Opts struct {
	Nats          *transport.NatsConnectionConfig
	Stream        string
	Subject       string `json:",omitempty"`
	RequestWaitMs uint
}

type Stream struct {
	opts        *Opts
	requestWait nats.MaxWait
	nc          *transport.NatsConnection
	js          nats.JetStreamContext

	info     *nats.StreamInfo
	infoErr  error
	infoTime time.Time
	infoMtx  sync.Mutex
}

func (opts Opts) FillMissingFields() *Opts {
	if opts.RequestWaitMs == 0 {
		opts.RequestWaitMs = 5000
	}
	return &opts
}

func ConnectStream(opts *Opts) (*Stream, error) {
	opts = opts.FillMissingFields()
	s := &Stream{
		opts:        opts,
		requestWait: nats.MaxWait(time.Millisecond * time.Duration(opts.RequestWaitMs)),
	}

	s.log("connecting to NATS...")
	var err error
	s.nc, err = transport.ConnectNATS(opts.Nats, nil)
	if err != nil {
		s.log("unable to connect to NATS: %v", err)
		s.Disconnect()
		return nil, err
	}

	s.log("connecting to NATS JetStream...")
	s.js, err = s.nc.Conn().JetStream(s.requestWait)
	if err != nil {
		s.log("unable to connect to NATS JetStream: %v", err)
		s.Disconnect()
		return nil, err
	}

	s.log("getting stream info...")
	info, _, err := s.GetInfo(0)
	if err != nil {
		s.log("unable to get stream info: %v", err)
		s.Disconnect()
		return nil, err
	}

	if len(opts.Subject) == 0 {
		s.log("subject is not specified, figuring it out automatically...")
		curInfo := info
		for curInfo.Config.Mirror != nil {
			mirrorName := curInfo.Config.Mirror.Name
			s.log("stream '%s' is mirrored from stream '%s', getting it's info...", curInfo.Config.Name, mirrorName)
			curInfo, err = s.js.StreamInfo(mirrorName, s.requestWait)
			if err != nil {
				s.log("unable to get stream '%s' info: %v", mirrorName, err)
				s.Disconnect()
				return nil, err
			}
		}

		if len(curInfo.Config.Subjects) == 0 {
			err := fmt.Errorf("stream '%s' has no subjects", curInfo.Config.Name)
			s.log(err.Error())
			s.Disconnect()
			return nil, err
		}

		opts.Subject = curInfo.Config.Subjects[0]
		s.log("subject '%s' is chosen", opts.Subject)
	}

	s.log("connected")

	return s, nil
}

func (s *Stream) Disconnect() error {
	if s.nc == nil {
		return nil
	}
	s.log("disconnecting...")
	err := s.nc.Drain()
	s.nc = nil
	return err
}

func (s *Stream) GetInfo(ttl time.Duration) (*nats.StreamInfo, time.Time, error) {
	s.infoMtx.Lock()
	defer s.infoMtx.Unlock()
	if ttl == 0 || time.Since(s.infoTime) > ttl {
		s.info, s.infoErr = s.js.StreamInfo(s.opts.Stream, s.requestWait)
		s.infoTime = time.Now()
	}
	return s.info, s.infoTime, s.infoErr
}

func (s *Stream) Get(seq uint64) (*nats.RawStreamMsg, error) {
	return s.js.GetMsg(s.opts.Stream, seq, s.requestWait)
}

func (s *Stream) Write(data []byte, msgId string, publishAckWait nats.AckWait) (*nats.PubAck, error) {
	header := make(nats.Header)
	header.Add(nats.MsgIdHdr, msgId)
	header.Add(nats.ExpectedStreamHdr, s.opts.Stream)
	msg := &nats.Msg{
		Subject: s.opts.Subject,
		Header:  header,
		Data:    data,
	}
	return s.js.PublishMsg(msg, publishAckWait)
}

func (s *Stream) log(format string, v ...any) {
	log.Printf(fmt.Sprintf("Stream [%s / %s]: ", s.opts.Nats.LogTag, s.opts.Stream)+format, v...)
}

func (s *Stream) Stats() *nats.Statistics {
	stats := s.nc.Conn().Stats()
	return &stats
}
