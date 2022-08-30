package metrics

import "github.com/prometheus/client_golang/prometheus"

func CreateHistogramBuckets(min float64, max float64, count uint, exponential bool) []float64 {
	if exponential {
		return prometheus.ExponentialBucketsRange(min, max, int(count))
	}

	width := (max - min) / float64(count)
	return prometheus.LinearBuckets(min, width, int(count))
}
