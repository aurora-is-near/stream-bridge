package metrics

import (
	_metrics "github.com/aurora-is-near/stream-bridge/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	Server _metrics.Server
	Labels map[string]string

	InputStream  *StreamMetrics `json:"-"`
	OutputStream *StreamMetrics `json:"-"`

	InputStartSeq   prometheus.Gauge `json:"-"`
	InputEndSeq     prometheus.Gauge `json:"-"`
	ToleranceWindow prometheus.Gauge `json:"-"`

	ReaderSeq    prometheus.Gauge `json:"-"`
	ReaderHeight prometheus.Gauge `json:"-"`

	WriterTipHeight prometheus.Gauge `json:"-"`

	LastWrittenHeight    prometheus.Gauge `json:"-"`
	LastWrittenInputSeq  prometheus.Gauge `json:"-"`
	LastWrittenOutputSeq prometheus.Gauge `json:"-"`

	ReadsCount             prometheus.Counter `json:"-"`
	WritesCount            prometheus.Counter `json:"-"`
	LowSeqSkipsCount       prometheus.Counter `json:"-"`
	CorruptedBlockSkips    prometheus.Counter `json:"-"`
	LowHeightSkipsCount    prometheus.Counter `json:"-"`
	HashMismatchSkipsCount prometheus.Counter `json:"-"`

	ConsecutiveWrongBlocks prometheus.Gauge `json:"-"`
}

func (m *Metrics) Start() error {
	labelNames := []string{}
	labelValues := []string{}
	for name, value := range m.Labels {
		labelNames = append(labelNames, name)
		labelValues = append(labelValues, value)
	}

	m.InputStream = createStreamMetrics("input", m.Server, labelNames, labelValues)
	m.OutputStream = createStreamMetrics("output", m.Server, labelNames, labelValues)

	m.InputStartSeq = m.Server.AddGauge(
		"input_start_seq",
		"Input start sequence (from configuration)",
		labelNames,
	).WithLabelValues(labelValues...)

	m.InputEndSeq = m.Server.AddGauge(
		"input_end_seq",
		"Input end sequence (from configuration)",
		labelNames,
	).WithLabelValues(labelValues...)

	m.ToleranceWindow = m.Server.AddGauge(
		"tolerance_window",
		"Maximum allowed number of consecutive wrong blocks (from config)",
		labelNames,
	).WithLabelValues(labelValues...)

	m.ReaderSeq = m.Server.AddGauge(
		"reader_seq",
		"Current seq of reader",
		labelNames,
	).WithLabelValues(labelValues...)

	m.ReaderHeight = m.Server.AddGauge(
		"reader_height",
		"Current height of reader",
		labelNames,
	).WithLabelValues(labelValues...)

	m.WriterTipHeight = m.Server.AddGauge(
		"writer_tip_height",
		"Height of the last (known by writer) block of output stream",
		labelNames,
	).WithLabelValues(labelValues...)

	m.LastWrittenHeight = m.Server.AddGauge(
		"last_written_height",
		"Height of the last written block",
		labelNames,
	).WithLabelValues(labelValues...)

	m.LastWrittenInputSeq = m.Server.AddGauge(
		"writer_last_written_input_seq",
		"Seq (input) of the last written block",
		labelNames,
	).WithLabelValues(labelValues...)

	m.LastWrittenOutputSeq = m.Server.AddGauge(
		"writer_last_written_output_seq",
		"Seq (output) of the last written block",
		labelNames,
	).WithLabelValues(labelValues...)

	m.ReadsCount = m.Server.AddCounter(
		"reads_count",
		"Number of reads",
		labelNames,
	).WithLabelValues(labelValues...)

	m.WritesCount = m.Server.AddCounter(
		"writes_count",
		"Number of writes",
		labelNames,
	).WithLabelValues(labelValues...)

	m.LowSeqSkipsCount = m.Server.AddCounter(
		"low_seq_skips_count",
		"Number of low seq skips",
		labelNames,
	).WithLabelValues(labelValues...)

	m.CorruptedBlockSkips = m.Server.AddCounter(
		"corrupted_block_skips",
		"Number of corrupted block skips",
		labelNames,
	).WithLabelValues(labelValues...)

	m.LowHeightSkipsCount = m.Server.AddCounter(
		"low_height_skips_count",
		"Number of low height skips",
		labelNames,
	).WithLabelValues(labelValues...)

	m.HashMismatchSkipsCount = m.Server.AddCounter(
		"hash_mismatch_skips_count",
		"Number of hash mismatch skips",
		labelNames,
	).WithLabelValues(labelValues...)

	m.ConsecutiveWrongBlocks = m.Server.AddGauge(
		"consecutive_wrong_blocks",
		"Number of consecutive wrong blocks",
		labelNames,
	).WithLabelValues(labelValues...)

	return m.Server.Start()
}

func (m *Metrics) Closed() <-chan error {
	return m.Server.Closed()
}

func (m *Metrics) Stop() {
	m.Server.Stop()
}
