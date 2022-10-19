package transport

import (
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type NatsConnectionConfig struct {
	Endpoints           []string
	Creds               string
	TimeoutMs           uint
	PingIntervalMs      uint
	MaxPingsOutstanding int
	LogTag              string
	Name                string `json:"-"`
}

type NatsConnection struct {
	cfg        *NatsConnectionConfig
	connection *nats.Conn
	closed     chan error
	errorChan  chan<- error
}

func (cfg NatsConnectionConfig) FillMissingFields() *NatsConnectionConfig {
	if cfg.TimeoutMs == 0 {
		cfg.TimeoutMs = 10000
	}
	if cfg.PingIntervalMs == 0 {
		cfg.PingIntervalMs = 600000
	}
	if cfg.MaxPingsOutstanding == 0 {
		cfg.MaxPingsOutstanding = 5
	}
	if len(cfg.LogTag) == 0 {
		cfg.LogTag = "unnamed"
	}
	if len(cfg.Name) == 0 {
		cfg.Name = "unnamed"
	}
	return &cfg
}

func (conn *NatsConnection) SetErrorChan(errorChan chan<- error) {
	conn.errorChan = errorChan
}

func ConnectNATS(cfg *NatsConnectionConfig, errorChan chan<- error) (*NatsConnection, error) {
	cfg = cfg.FillMissingFields()

	conn := &NatsConnection{
		cfg:       cfg,
		closed:    make(chan error, 1),
		errorChan: errorChan,
	}

	options := []nats.Option{
		nats.Name(cfg.Name),
		nats.ReconnectWait(time.Second / 5),
		nats.PingInterval(time.Duration(cfg.PingIntervalMs) * time.Millisecond),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.MaxPingsOutstanding(cfg.MaxPingsOutstanding),
		nats.Timeout(time.Duration(cfg.TimeoutMs) * time.Millisecond),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			log.Printf("NATS [%v] error: %v", cfg.LogTag, err)
			if conn.errorChan != nil {
				conn.errorChan <- err
			}
		}),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				log.Printf("NATS [%v] disconnected due to: %v, will try reconnecting", cfg.LogTag, err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("NATS [%v] reconnected [%v]", cfg.LogTag, nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			err := nc.LastError()
			if err != nil {
				log.Printf("NATS [%v] connection closed: %v", cfg.LogTag, err)
			}
			conn.closed <- err
		}),
	}
	if len(cfg.Creds) > 0 {
		options = append(options, nats.UserCredentials(cfg.Creds))
	}

	log.Printf("Connecting to NATS [%v]...", cfg.LogTag)
	var err error
	conn.connection, err = nats.Connect(strings.Join(cfg.Endpoints, ", "), options...)

	return conn, err
}

func (conn *NatsConnection) Drain() error {
	log.Printf("Draining NATS [%v] connection...", conn.cfg.LogTag)
	if err := conn.connection.Drain(); err != nil {
		log.Printf("Error when draining NATS [%v] connection: %v", conn.cfg.LogTag, err)
		return err
	}
	return nil
}

func (conn *NatsConnection) Conn() *nats.Conn {
	return conn.connection
}

func (conn *NatsConnection) Closed() <-chan error {
	return conn.closed
}
