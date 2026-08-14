package cli

import (
	"math"
	"testing"
)

// TestParallelismAsInt guards the uint->int conversion used by the --parallel
// flags. A plain int(v) would wrap large values to a negative int, which is the
// panic-on-negative-parallelism bug that kopia/kopia#2022 fixed.
func TestParallelismAsInt(t *testing.T) {
	cases := []struct {
		name  string
		input uint
		want  int
	}{
		{name: "zero", input: 0, want: 0},
		{name: "one", input: 1, want: 1},
		{name: "typical", input: 16, want: 16},
		{name: "maxint32", input: math.MaxInt32, want: math.MaxInt32},
		{name: "above maxint32 clamps", input: math.MaxInt32 + 1, want: math.MaxInt32},
		{name: "maxuint does not wrap negative", input: math.MaxUint, want: math.MaxInt32},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parallelismAsInt(tc.input); got != tc.want {
				t.Fatalf("parallelismAsInt(%d) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}
