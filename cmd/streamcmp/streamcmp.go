package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aurora-is-near/stream-bridge/stream"
	"github.com/aurora-is-near/stream-bridge/streamcmp"
	"github.com/aurora-is-near/stream-bridge/transport"
)

var (
	server                = flag.String("server", "", "NATS server URL")
	creds                 = flag.String("creds", "", "path to NATS credentials file")
	consumer              = flag.String("consumer", "", "NATS JetStream durable consumer name")
	streamA               = flag.String("stream-a", "", "stream A name")
	seqA                  = flag.Uint64("seq-a", 1, "start sequence on stream A")
	streamB               = flag.String("stream-b", "", "stream B name")
	seqB                  = flag.Uint64("seq-b", 1, "start sequence on stream B")
	mode                  = flag.String("mode", "simple", "must be one of ['simple', 'aurora', 'near']")
	batchSize             = flag.Uint("batch", 500, "max request batch size")
	rps                   = flag.Float64("rps", 1, "max requests per second")
	skipDuplicates        = flag.Bool("skip-duplicates", false, "")
	skipUnequalDuplicates = flag.Bool("skip-unequal-duplicates", false, "")
	skipGaps              = flag.Bool("skip-gaps", false, "")
	skipDownjumps         = flag.Bool("skip-downjumps", false, "")
	skipCorrupted         = flag.Bool("skip-corrupted", false, "")
	skipDiscrepancy       = flag.Bool("skip-discrepancy", false, "")
)

func main() {
	flag.Parse()

	if len(*server) == 0 {
		log.Fatal("-server must be specified")
	}
	if len(*consumer) == 0 {
		log.Fatal("-consumer must be specified")
	}
	if len(*streamA) == 0 {
		log.Fatal("-stream-a must be specified")
	}
	if len(*streamB) == 0 {
		log.Fatal("-stream-b must be specified")
	}
	if *mode = strings.ToLower(*mode); *mode != "simple" && *mode != "aurora" && *mode != "near" {
		log.Fatal("-mode must be one of ['simple', 'aurora', 'near']")
	}

	sc := &streamcmp.StreamCmp{
		Mode: *mode,
		StreamA: &stream.AutoReader{
			Stream: &stream.Opts{
				Nats: &transport.NatsConnectionConfig{
					Endpoints: []string{*server},
					Creds:     *creds,
					LogTag:    "A",
					Name:      "streamcmp",
				},
				Stream: *streamA,
			},
			Reader: &stream.ReaderOpts{
				MaxRps:                  *rps,
				BufferSize:              1000,
				MaxRequestBatchSize:     *batchSize,
				SortBatch:               true,
				Durable:                 *consumer + "_a",
				StrictStart:             true,
				WrongSeqToleranceWindow: 10,
			},
			ReconnectWaitMs: 2000,
		},
		StreamB: &stream.AutoReader{
			Stream: &stream.Opts{
				Nats: &transport.NatsConnectionConfig{
					Endpoints: []string{*server},
					Creds:     *creds,
					LogTag:    "B",
					Name:      "streamcmp",
				},
				Stream: *streamB,
			},
			Reader: &stream.ReaderOpts{
				MaxRps:                  *rps,
				BufferSize:              1000,
				MaxRequestBatchSize:     *batchSize,
				SortBatch:               true,
				Durable:                 *consumer + "_b",
				StrictStart:             true,
				WrongSeqToleranceWindow: 10,
			},
			ReconnectWaitMs: 2000,
		},
		StartSeqA:             *seqA,
		StartSeqB:             *seqB,
		SkipDuplicates:        *skipDuplicates,
		SkipUnequalDuplicates: *skipUnequalDuplicates,
		SkipGaps:              *skipGaps,
		SkipDownjumps:         *skipDownjumps,
		SkipCorrupted:         *skipCorrupted,
		SkipDiscrepancy:       *skipDiscrepancy,
	}
	if err := sc.Run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}
