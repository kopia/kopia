package cli

import "math"

// parallelismAsInt converts --parallel-style flag values from
// uint to int and clamps returned values at MaxInt32.
func parallelismAsInt(v uint) int {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}

	return int(v)
}
