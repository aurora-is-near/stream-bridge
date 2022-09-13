package metrics

import (
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	_metrics "github.com/aurora-is-near/stream-bridge/metrics"
	"github.com/aurora-is-near/stream-bridge/util"
	"github.com/olekukonko/tablewriter"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

type Metrics struct {
	Server                _metrics.Server
	Labels                map[string]string
	StdoutIntervalSeconds uint

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

	stdoutStop chan struct{}
	stdoutWg   sync.WaitGroup
}

func (m *Metrics) Start() error {
	labelNames := []string{}
	labelValues := []string{}
	for name, value := range m.Labels {
		labelNames = append(labelNames, name)
		labelValues = append(labelValues, value)
	}

	m.InputStream = createStreamMetrics("input", &m.Server, labelNames, labelValues)
	m.OutputStream = createStreamMetrics("output", &m.Server, labelNames, labelValues)

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

	if err := m.Server.Start(); err != nil {
		return err
	}

	m.stdoutStop = make(chan struct{})
	if m.StdoutIntervalSeconds > 0 {
		m.stdoutWg.Add(1)
		go m.stdoutLoop()
	}

	return nil
}

func (m *Metrics) Closed() <-chan error {
	return m.Server.Closed()
}

func (m *Metrics) Stop() {
	close(m.stdoutStop)
	m.stdoutWg.Wait()
	m.Server.Stop()
}

func (m *Metrics) stdoutLoop() {
	defer m.stdoutWg.Done()

	ticker := time.NewTicker(time.Second * time.Duration(m.StdoutIntervalSeconds))
	defer ticker.Stop()

	for {
		select {
		case <-m.stdoutStop:
			return
		default:
		}

		select {
		case <-m.stdoutStop:
			return
		case <-ticker.C:
			m.spewStdout()
		}
	}
}

func (m *Metrics) spewStdout() {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"", "INPUT_SEQ", "OUTPUT_SEQ", "INPUT_HEIGHT", "OUTPUT_HEIGHT"})
	table.AppendBulk([][]string{
		{"LAST", maxGauge(m.InputStream.LastSeq, m.ReaderSeq), maxGauge(m.OutputStream.LastSeq, m.LastWrittenOutputSeq), "", maxGauge(m.WriterTipHeight)},
		{"READER", maxGauge(m.ReaderSeq), "", maxGauge(m.ReaderHeight), ""},
		{"WRITER", maxGauge(m.LastWrittenInputSeq), maxGauge(m.LastWrittenOutputSeq), maxGauge(m.LastWrittenHeight), maxGauge(m.LastWrittenHeight)},
		{"FIRST", maxGauge(m.InputStream.FirstSeq), maxGauge(m.OutputStream.FirstSeq), "", ""},
	})
	table.Render()
}

func maxGauge(gs ...prometheus.Gauge) string {
	max := uint64(0)
	for _, g := range gs {
		var out io_prometheus_client.Metric
		if err := g.Write(&out); err != nil {
			log.Printf("Metric2stdout: can't read metric: %v", err)
			return "error"
		}
		max = util.Max(max, uint64(out.Gauge.GetValue()))
	}
	return strconv.FormatUint(max, 10)
}
