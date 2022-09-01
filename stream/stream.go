package stream

import (
	"log"
	"time"

	"github.com/aurora-is-near/stream-bridge/transport"
	"github.com/nats-io/nats.go"
)

type Stream struct {
	Nats             *transport.NatsConnectionConfig
	Stream           string
	Subject          string
	RequestWaitMs    uint
	PublishAckWaitMs uint

	requestWait    nats.MaxWait
	publishAckWait nats.AckWait
	nc             *transport.NatsConnection
	js             nats.JetStreamContext
}

func (s *Stream) Connect() (*nats.StreamInfo, error) {
	if s.RequestWaitMs == 0 {
		s.RequestWaitMs = 5000
	}
	s.requestWait = nats.MaxWait(time.Millisecond * time.Duration(s.RequestWaitMs))

	if s.PublishAckWaitMs == 0 {
		s.PublishAckWaitMs = 5000
	}
	s.publishAckWait = nats.AckWait(time.Millisecond * time.Duration(s.PublishAckWaitMs))

	if s.nc != nil {
		log.Printf("Stream [%v]: reconnecting...", s.Nats.LogTag)
		s.nc.Drain()
		s.nc, s.js = nil, nil
	} else {
		log.Printf("Stream [%v]: connecting...", s.Nats.LogTag)
	}

	log.Printf("Stream [%v]: connecting to NATS...", s.Nats.LogTag)
	var err error
	s.nc, err = transport.ConnectNATS(s.Nats, nil)
	if err != nil {
		log.Printf("Stream [%v]: unable to connect to NATS: %v", s.Nats.LogTag, err)
		s.nc, s.js = nil, nil
		return nil, err
	}

	log.Printf("Stream [%v]: connecting to NATS JetStream...", s.Nats.LogTag)
	s.js, err = s.nc.Conn().JetStream(s.requestWait)
	if err != nil {
		log.Printf("Stream [%v]: unable to connect to NATS JetStream: %v", s.Nats.LogTag, err)
		s.nc.Drain()
		s.nc, s.js = nil, nil
		return nil, err
	}

	log.Printf("Stream [%v]: getting stream info...", s.Nats.LogTag)
	info, err := s.GetStreamInfo()
	if err != nil {
		log.Printf("Stream [%v]: unable to get stream info: %v", s.Nats.LogTag, err)
		s.nc.Drain()
		s.nc, s.js = nil, nil
		return nil, err
	}

	log.Printf("Stream [%v]: connected", s.Nats.LogTag)

	return info, err
}

func (s *Stream) Disconnect() error {
	if s.nc == nil {
		return nil
	}
	log.Printf("Stream [%v]: disconnecting...", s.Nats.LogTag)
	err := s.nc.Drain()
	s.nc, s.js = nil, nil
	return err
}

func (s *Stream) GetStreamInfo() (*nats.StreamInfo, error) {
	return s.js.StreamInfo(s.Stream, s.requestWait)
}

func (s *Stream) Get(seq uint64) (*nats.RawStreamMsg, error) {
	return s.js.GetMsg(s.Stream, seq, s.requestWait)
}

func (s *Stream) Write(data []byte, msgId string) (*nats.PubAck, error) {
	header := make(nats.Header)
	header.Add(nats.MsgIdHdr, msgId)
	header.Add(nats.ExpectedStreamHdr, s.Stream)
	msg := &nats.Msg{
		Subject: s.Subject,
		Header:  header,
		Data:    data,
	}
	return s.js.PublishMsg(msg, s.publishAckWait)
}
