package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aurora-is-near/stream-backup/chunks"
	"github.com/aurora-is-near/stream-bridge/blockwriter"
	"github.com/aurora-is-near/stream-bridge/stream"
	"github.com/aurora-is-near/stream-bridge/streamrestore"
	"github.com/aurora-is-near/stream-bridge/transport"
)

var (
	server         = flag.String("server", "", "NATS server URL")
	creds          = flag.String("creds", "", "path to NATS credentials file")
	streamName     = flag.String("stream", "", "output stream name")
	seqStart       = flag.Uint64("seq-start", 1, "start sequence on stream")
	seqEnd         = flag.Uint64("seq-end", 99999999999, "end sequence on stream")
	mode           = flag.String("mode", "unverified", "must be one of ['unverified', 'aurora', 'near']")
	dir            = flag.String("dir", "backup", "output dir")
	backup         = flag.String("backup", "", "(chunks prefix before underscore)")
	tolerance      = flag.Uint("tolerance", 1000, "tolerance window (how many consequent wrong blocks can be ignored)")
	noExpectHeight = flag.Uint64("no-expect-height", 0, "don't set expect-last-<...> headers on this height")
	noExpectSeq    = flag.Uint64("no-expect-sequence", 0, "don't set expect-last-<...> headers on this sequence")
)

func main() {
	flag.Parse()

	if len(*server) == 0 {
		log.Fatal("-server must be specified")
	}
	if len(*streamName) == 0 {
		log.Fatal("-stream must be specified")
	}
	if *mode = strings.ToLower(*mode); *mode != "unverified" && *mode != "aurora" && *mode != "near" {
		log.Fatal("-mode must be one of ['unverified', 'aurora', 'near']")
	}

	sr := &streamrestore.StreamRestore{
		Mode: *mode,
		Chunks: &chunks.Chunks{
			Dir:             *dir,
			ChunkNamePrefix: *backup + "_",
		},
		Stream: &stream.Opts{
			Nats: &transport.NatsConnectionConfig{
				Endpoints: []string{*server},
				Creds:     *creds,
				LogTag:    "restore",
				Name:      "streamrestore",
			},
			Stream: *streamName,
		},
		Writer: &blockwriter.Opts{
			PublishAckWaitMs:           10_000,
			MaxWriteAttempts:           3,
			WriteRetryWaitMs:           3_000,
			TipTtlSeconds:              15,
			DisableExpectedCheck:       *noExpectSeq,
			DisableExpectedCheckHeight: *noExpectHeight,
		},
		StartSeq:        *seqStart,
		EndSeq:          *seqEnd,
		ToleranceWindow: *tolerance,
		ReconnectWaitMs: 2_000,
	}

	if err := sr.Run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}
