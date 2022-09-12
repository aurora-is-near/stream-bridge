package util

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
