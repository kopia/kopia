package ecc

import "math"

func ComputeShards(spaceUsedPercentage float32) (required, redundant int) {
	required = 128
	redundant = between(applyPercent(required, spaceUsedPercentage/100), 1, 128)

	if redundant == 1 {
		redundant = 2
		required = between(applyPercent(redundant, 100/spaceUsedPercentage), 128, 254)
	}

	// Berlekamp-Welch error correction works better with an even number
	if redundant%2 == 1 {
		redundant++
	}

	return
}

func between(val int, min int, max int) int {
	switch {
	case val < min:
		return min
	case val > max:
		return max
	default:
		return val
	}
}

func applyPercent(val int, percent float32) int {
	return int(math.Floor(float64(val) * float64(percent)))
}

func clear(bytes []byte) {
	for i := range bytes {
		bytes[i] = 0
	}
}

func min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

func CeilInt(a, b int) int {
	return int(math.Ceil(float64(a) / float64(b)))
}