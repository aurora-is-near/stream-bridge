package metrics

import (
	"log"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	ListenAddress string
	Namespace     string
	Subsystem     string

	metrics    []prometheus.Collector
	httpServer *http.Server
	closed     chan error
	wg         sync.WaitGroup
}

func (server *Server) AddGauge(name string, help string, labelNames []string) *prometheus.GaugeVec {
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: server.Namespace,
			Subsystem: server.Subsystem,
			Name:      name,
			Help:      help,
		},
		labelNames,
	)
	server.metrics = append(server.metrics, gauge)
	return gauge
}

func (server *Server) AddCounter(name string, help string, labelNames []string) *prometheus.CounterVec {
	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: server.Namespace,
			Subsystem: server.Subsystem,
			Name:      name,
			Help:      help,
		},
		labelNames,
	)
	server.metrics = append(server.metrics, counter)
	return counter
}

func (server *Server) AddHistogram(name string, help string, buckets []float64, labelNames []string) *prometheus.HistogramVec {
	histogram := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: server.Namespace,
			Subsystem: server.Subsystem,
			Name:      name,
			Buckets:   buckets,
			Help:      help,
		},
		labelNames,
	)
	server.metrics = append(server.metrics, histogram)
	return histogram
}

func (server *Server) Start() error {
	log.Printf("Metrics Server: starting...")

	registry := prometheus.NewRegistry()
	for _, metric := range server.metrics {
		if err := registry.Register(metric); err != nil {
			return err
		}
	}

	server.httpServer = &http.Server{
		Addr:    server.ListenAddress,
		Handler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	}

	server.closed = make(chan error, 1)
	server.wg.Add(1)

	go func() {
		server.closed <- server.httpServer.ListenAndServe()
		server.wg.Done()
	}()

	return nil
}

func (server *Server) Closed() <-chan error {
	return server.closed
}

func (server *Server) Stop() {
	log.Printf("Metrics Server: stopping...")
	server.httpServer.Close()
	server.wg.Wait()
}
