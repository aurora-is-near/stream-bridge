package streambridge

func boolMetric(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func Max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func Min(a uint64, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
