package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aurora-is-near/stream-bridge/stream"
	"github.com/aurora-is-near/stream-bridge/streambackup"
	"github.com/aurora-is-near/stream-bridge/streambackup/chunks"
	"github.com/aurora-is-near/stream-bridge/transport"
)

var (
	server          = flag.String("server", "", "NATS server URL")
	creds           = flag.String("creds", "", "path to NATS credentials file")
	consumer        = flag.String("consumer", "", "NATS JetStream durable consumer name")
	streamName      = flag.String("stream", "", "stream name")
	seqStart        = flag.Uint64("seq-start", 1, "start sequence on stream")
	seqEnd          = flag.Uint64("seq-end", 99999999999, "end sequence on stream")
	mode            = flag.String("mode", "simple", "must be one of ['simple', 'aurora', 'near']")
	batchSize       = flag.Uint("batch", 500, "max request batch size")
	rps             = flag.Float64("rps", 1, "max requests per second")
	dir             = flag.String("dir", "backup", "output dir")
	compress        = flag.Int("compress", 9, "compression level [0-9]")
	maxChunkEntries = flag.Int("max-chunk-entries", 1_000_000, "max entries per chunk")
	maxChunkSize    = flag.Int("max-chunk-size", 1024, "max chunk size in megabytes (before compression)")
)

func main() {
	flag.Parse()

	if len(*server) == 0 {
		log.Fatal("-server must be specified")
	}
	if len(*creds) == 0 {
		log.Fatal("-creds must be specified")
	}
	if len(*consumer) == 0 {
		log.Fatal("-consumer must be specified")
	}
	if len(*streamName) == 0 {
		log.Fatal("-stream must be specified")
	}
	if *mode = strings.ToLower(*mode); *mode != "simple" && *mode != "aurora" && *mode != "near" {
		log.Fatal("-mode must be one of ['simple', 'aurora', 'near']")
	}

	sb := &streambackup.StreamBackup{
		Mode: *mode,
		Chunks: &chunks.Chunks{
			Dir:                *dir,
			ChunkNamePrefix:    *streamName + "_",
			CompressionLevel:   *compress,
			MaxEntriesPerChunk: *maxChunkEntries,
			MaxChunkSize:       *maxChunkSize * 1024 * 1024,
		},
		Reader: &stream.AutoReader{
			Stream: &stream.Opts{
				Nats: &transport.NatsConnectionConfig{
					Endpoints: []string{*server},
					Creds:     *creds,
					LogTag:    "backup",
				},
				Stream: *streamName,
			},
			Reader: &stream.ReaderOpts{
				MaxRps:                  *rps,
				BufferSize:              1000,
				MaxRequestBatchSize:     *batchSize,
				SortBatch:               true,
				Durable:                 *consumer,
				StrictStart:             true,
				WrongSeqToleranceWindow: 10,
			},
			ReconnectWaitMs: 2000,
		},
		StartSeq: *seqStart,
		EndSeq:   *seqEnd,
	}

	if err := sb.Run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}
