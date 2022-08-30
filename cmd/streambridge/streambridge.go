package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	_metrics "github.com/aurora-is-near/stream-bridge/metrics"
	"github.com/aurora-is-near/stream-bridge/streambridge"
	"github.com/aurora-is-near/stream-bridge/streambridge/metrics"
	"github.com/aurora-is-near/stream-bridge/transport"
)

var config = &streambridge.StreamBridge{
	Mode: "aurora",
	InputNats: &transport.NatsConnectionConfig{
		Endpoints: []string{
			"tls://input.dev:4222",
		},
		Creds:               "nats.creds",
		TimeoutMs:           10000,
		PingIntervalMs:      600000,
		MaxPingsOutstanding: 5,
		LogTag:              "input",
	},
	OutputNats: &transport.NatsConnectionConfig{
		Endpoints: []string{
			"tls://output.dev:4222",
		},
		Creds:               "nats.creds",
		TimeoutMs:           10000,
		PingIntervalMs:      600000,
		MaxPingsOutstanding: 5,
		LogTag:              "output",
	},
	InputStream:   "myblocks",
	InputSubject:  "myblocks",
	OutputStream:  "myblocks",
	OutputSubject: "myblocks",
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
