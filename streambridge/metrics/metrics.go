package metrics

import (
	_metrics "github.com/aurora-is-near/stream-bridge/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	Server _metrics.Server
	Labels map[string]string

	InputStreamConnected       prometheus.Gauge   `json:"-"`
	InputStreamSequenceNumber  prometheus.Gauge   `json:"-"`
	InputStreamBlockHeight     prometheus.Gauge   `json:"-"`
	OutputStreamConnected      prometheus.Gauge   `json:"-"`
	OutputStreamSequenceNumber prometheus.Gauge   `json:"-"`
	OutputStreamBlockHeight    prometheus.Gauge   `json:"-"`
	CatchUpSkips               prometheus.Counter `json:"-"`
	CorruptedSkips             prometheus.Counter `json:"-"`
	HashMismatchSkips          prometheus.Counter `json:"-"`
}

func (m *Metrics) Start() error {
	labelNames := []string{}
	labelValues := []string{}
	for name, value := range m.Labels {
		labelNames = append(labelNames, name)
		labelValues = append(labelValues, value)
	}

	m.InputStreamConnected = m.Server.AddGauge(
		"input_stream_connected",
		"Is input stream connected (0 or 1)",
		labelNames,
	).WithLabelValues(labelValues...)

	m.InputStreamSequenceNumber = m.Server.AddGauge(
		"input_stream_sequence_number",
		"Sequence number on the input stream",
		labelNames,
	).WithLabelValues(labelValues...)

	m.InputStreamBlockHeight = m.Server.AddGauge(
		"input_stream_block_height",
		"Block height on the input stream",
		labelNames,
	).WithLabelValues(labelValues...)

	m.OutputStreamConnected = m.Server.AddGauge(
		"output_stream_connected",
		"Is output stream connected (0 or 1)",
		labelNames,
	).WithLabelValues(labelValues...)

	m.OutputStreamSequenceNumber = m.Server.AddGauge(
		"output_stream_sequence_number",
		"Sequence number on the output stream",
		labelNames,
	).WithLabelValues(labelValues...)

	m.OutputStreamBlockHeight = m.Server.AddGauge(
		"output_stream_block_height",
		"Block height on the output stream",
		labelNames,
	).WithLabelValues(labelValues...)

	m.CatchUpSkips = m.Server.AddCounter(
		"catch_up_skips",
		"Input stream catch up skips count",
		labelNames,
	).WithLabelValues(labelValues...)

	m.CorruptedSkips = m.Server.AddCounter(
		"corrupted_skips",
		"Input stream corrupted blocks skips count",
		labelNames,
	).WithLabelValues(labelValues...)

	m.HashMismatchSkips = m.Server.AddCounter(
		"hash_mismatch_skips",
		"Input stream hash mismatch skips count",
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
