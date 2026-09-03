package cli

import "math"

// parallelismAsInt converts a --parallel-style flag value to the int that the
// underlying APIs take.
//
// The flags are declared as uint so kingpin rejects negative input at parse
// time, which is the fix for kopia/kopia#2022. A plain int(v) conversion would
// undo that on 64-bit platforms: any value above math.MaxInt64 wraps to a
// negative int, handing the callee exactly the negative parallelism that used
// to panic. Clamping keeps the conversion total, and MaxInt32 is far beyond any
// useful degree of parallelism.
func parallelismAsInt(v uint) int {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}

	return int(v)
}
