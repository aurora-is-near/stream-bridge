package metrics

import (
	"fmt"
	"sync"
	"time"

	_metrics "github.com/aurora-is-near/stream-bridge/metrics"
	"github.com/aurora-is-near/stream-bridge/stream"
	"github.com/prometheus/client_golang/prometheus"
)

// Default prometheus fetch interval
const streamObserveInterval = time.Second * 15

type StreamMetrics struct {
	Connected prometheus.Gauge `json:"-"`
	FirstSeq  prometheus.Gauge `json:"-"`
	LastSeq   prometheus.Gauge `json:"-"`
	InBytes   prometheus.Gauge `json:"-"`
	OutBytes  prometheus.Gauge `json:"-"`

	s    stream.StreamInterface
	stop chan struct{}
	wg   sync.WaitGroup
}

func createStreamMetrics(streamName string, server *_metrics.Server, labelNames []string, labelValues []string) *StreamMetrics {
	return &StreamMetrics{
		Connected: server.AddGauge(
			fmt.Sprintf("%v_connected", streamName),
			fmt.Sprintf("Is %v stream connected (0 or 1)", streamName),
			labelNames,
		).WithLabelValues(labelValues...),

		FirstSeq: server.AddGauge(
			fmt.Sprintf("%v_first_seq", streamName),
			fmt.Sprintf("StreamInfo(%v).FirstSeq", streamName),
			labelNames,
		).WithLabelValues(labelValues...),

		LastSeq: server.AddGauge(
			fmt.Sprintf("%v_last_seq", streamName),
			fmt.Sprintf("StreamInfo(%v).LastSeq", streamName),
			labelNames,
		).WithLabelValues(labelValues...),

		InBytes: server.AddGauge(
			fmt.Sprintf("%v_in_bytes", streamName),
			fmt.Sprintf("%v NATS connection total bytes received", streamName),
			labelNames,
		).WithLabelValues(labelValues...),

		OutBytes: server.AddGauge(
			fmt.Sprintf("%v_out_bytes", streamName),
			fmt.Sprintf("%v NATS connection total bytes sent", streamName),
			labelNames,
		).WithLabelValues(labelValues...),
	}
}

func (sm *StreamMetrics) StartObserving(s stream.StreamInterface) {
	sm.s = s
	sm.Connected.Set(1)
	sm.stop = make(chan struct{})
	sm.wg.Add(1)
	go sm.observe()
}

func (sm *StreamMetrics) StopObserving() {
	sm.Connected.Set(0)
	close(sm.stop)
	sm.wg.Wait()
}

func (sm *StreamMetrics) observe() {
	defer sm.wg.Done()

	ticker := time.NewTicker(streamObserveInterval)
	defer ticker.Stop()

	sm.update()

	for {
		select {
		case <-sm.stop:
			return
		default:
		}

		select {
		case <-sm.stop:
			return
		case <-ticker.C:
			sm.update()
		}
	}
}

func (sm *StreamMetrics) update() {
	info, _, err := sm.s.GetInfo(streamObserveInterval / 2)
	if err == nil {
		sm.FirstSeq.Set(float64(info.State.FirstSeq))
		sm.LastSeq.Set(float64(info.State.LastSeq))
	}
	stats := sm.s.Stats()
	sm.InBytes.Set(float64(stats.InBytes))
	sm.OutBytes.Set(float64(stats.OutBytes))
}
