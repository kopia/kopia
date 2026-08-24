package maintenancestats

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToUint64(t *testing.T) {
	cases := []struct {
		in       int
		expected uint64
	}{
		{
			in:       math.MinInt,
			expected: 0,
		},
		{
			in:       -1,
			expected: 0,
		},
		{
			in:       0,
			expected: 0,
		},
		{
			in:       1,
			expected: 1,
		},
		{
			in:       math.MaxInt,
			expected: math.MaxInt,
		},
	}

	for _, c := range cases {
		v := ToUint64(c.in)
		require.Equal(t, c.expected, v)
	}
}
