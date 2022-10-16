package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/aurora-is-near/stream-bridge/blockwriter"
	_metrics "github.com/aurora-is-near/stream-bridge/metrics"
	"github.com/aurora-is-near/stream-bridge/stream"
	"github.com/aurora-is-near/stream-bridge/streambridge"
	"github.com/aurora-is-near/stream-bridge/streambridge/metrics"
	"github.com/aurora-is-near/stream-bridge/transport"
)

var config = &streambridge.StreamBridge{
	Mode: "aurora",
	Input: &stream.Opts{
		Nats: &transport.NatsConnectionConfig{
			Endpoints: []string{
				"tls://input.dev:4222",
			},
			Creds:               "nats.creds",
			TimeoutMs:           10000,
			PingIntervalMs:      600000,
			MaxPingsOutstanding: 5,
			LogTag:              "input",
		},
		Stream:        "myblocks",
		Subject:       "myblocks",
		RequestWaitMs: 5000,
	},
	Output: &stream.Opts{
		Nats: &transport.NatsConnectionConfig{
			Endpoints: []string{
				"tls://output.dev:4222",
			},
			Creds:               "nats.creds",
			TimeoutMs:           10000,
			PingIntervalMs:      600000,
			MaxPingsOutstanding: 5,
			LogTag:              "output",
		},
		Stream:        "myblocks",
		Subject:       "myblocks",
		RequestWaitMs: 5000,
	},
	Reader: &stream.ReaderOpts{
		MaxRps:                       2,
		BufferSize:                   1000,
		MaxRequestBatchSize:          250,
		SubscribeAckWaitMs:           5000,
		InactiveThresholdSeconds:     300,
		FetchTimeoutMs:               10000,
		SortBatch:                    true,
		LastSeqUpdateIntervalSeconds: 5,
		Durable:                      "myconsumer",
		StrictStart:                  false,
		WrongSeqToleranceWindow:      50,
	},
	Writer: &blockwriter.Opts{
		PublishAckWaitMs:     5000,
		MaxWriteAttempts:     3,
		WriteRetryWaitMs:     1000,
		TipTtlSeconds:        60,
		DisableExpectedCheck: 0,
	},
	InputStartSequence: 0,
	InputEndSequenece:  0,
	RestartDelayMs:     2000,
	ToleranceWindow:    1000,
	Metrics: &metrics.Metrics{
		Server: _metrics.Server{
			ListenAddress: "localhost:9991",
			Namespace:     "infra",
			Subsystem:     "stream_bridge",
		},
		Labels: map[string]string{
			"inputcluster":  "X",
			"outputcluster": "Y",
			"stream":        "myblocks",
			"whatever":      "whatever",
		},
		StdoutIntervalSeconds: 10,
	},
}

func main() {
	if len(os.Args) < 2 {
		d, _ := json.MarshalIndent(config, "", "  ")
		_, _ = fmt.Fprintf(os.Stdout, "%s\n", string(d))
		os.Exit(1)
	}
	d, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error reading config file: %s\n", err)
		os.Exit(1)
	}
	config = &streambridge.StreamBridge{}
	if err := json.Unmarshal(d, config); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error parsing config file: %s\n", err)
		os.Exit(1)
	}
	if err := config.Run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}
