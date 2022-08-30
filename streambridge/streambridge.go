package streambridge

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/aurora-is-near/stream-bridge/streambridge/metrics"
	"github.com/aurora-is-near/stream-bridge/transport"
)

type StreamBridge struct {
	Mode          string
	InputNats     *transport.NatsConnectionConfig
	OutputNats    *transport.NatsConnectionConfig
	InputStream   string
	InputSubject  string
	OutputStream  string
	OutputSubject string
	Metrics       *metrics.Metrics
}

func (sb *StreamBridge) Run() error {
	if err := sb.Metrics.Start(); err != nil {
		return err
	}
	defer sb.Metrics.Stop()

	interrupt := make(chan os.Signal, 10)
	signal.Notify(interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT, syscall.SIGUSR1)

	for {
		sync := StartSync(sb)
		select {
		case <-interrupt:
			sync.Stop()
			return nil
		case err := <-sb.Metrics.Closed():
			sync.Stop()
			return err
		case stopReason := <-sync.Stopped():
			if !stopReason.Recoverable {
				return stopReason.Error
			}
		}
	}
}
