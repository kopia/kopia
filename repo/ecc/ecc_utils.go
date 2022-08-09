package ecc

import "math"

func computeShards(spaceOverhead float32) (data, parity int) {
	// It's recommended to have at least 2 parity shards.
	// So the approach here is: we start with 128 data shards and compute
	// how many shards parity shards are needed for the selected space overhead.
	// If it turns out it is only 1, we invert the logic and compute how many
	// data shards are needed for 2 parity shards.
	data = 128
	parity = between(applyPercent(data, spaceOverhead/100), 1, 128) //nolint:gomnd

	if parity == 1 {
		parity = 2
		data = between(applyPercent(parity, 100/spaceOverhead), 128, 254) //nolint:gomnd
	}

	return
}

func between(val, min, max int) int {
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

func minInt(a, b int) int {
	if a <= b {
		return a
	}

	return b
}

func maxInt(a, b int) int {
	if a >= b {
		return a
	}

	return b
}

func maxFloat32(a, b float32) float32 {
	if a >= b {
		return a
	}

	return b
}

func ceilInt(a, b int) int {
	return int(math.Ceil(float64(a) / float64(b)))
}
