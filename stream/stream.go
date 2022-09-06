package stream

import (
	"fmt"
	"log"
	"time"

	"github.com/aurora-is-near/stream-bridge/transport"
	"github.com/nats-io/nats.go"
)

type Stream struct {
	Nats             *transport.NatsConnectionConfig
	Stream           string
	Subject          string `json:",omitempty"`
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
		s.log("reconnecting...")
		s.Disconnect()
	} else {
		s.log("connecting...")
	}

	s.log("connecting to NATS...")
	var err error
	s.nc, err = transport.ConnectNATS(s.Nats, nil)
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
	info, err := s.GetStreamInfo()
	if err != nil {
		s.log("unable to get stream info: %v", err)
		s.Disconnect()
		return nil, err
	}

	if len(s.Subject) == 0 {
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

		s.Subject = curInfo.Config.Subjects[0]
		s.log("subject '%s' is chosen", s.Subject)
	}

	s.log("connected")

	return info, nil
}

func (s *Stream) Disconnect() error {
	if s.nc == nil {
		return nil
	}
	s.log("disconnecting...")
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

func (s *Stream) log(format string, v ...any) {
	log.Printf(fmt.Sprintf("Stream [%s / %s]: ", s.Nats.LogTag, s.Stream)+format, v...)
}
