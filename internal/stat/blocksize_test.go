package stat_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/stat"
)

const (
	maxuint64 uint64 = ^uint64(0)
	maxint64         = int64(maxuint64 >> 1)
)

func TestGetBlockAlignedSizeSize(t *testing.T) {
	const blockSize4k int64 = 4096

	cases := []struct {
		blockSize int64
		size      int64
		expected  int64
	}{
		{
			blockSize: blockSize4k,
			size:      0,
			expected:  0,
		},
		{
			blockSize: blockSize4k,
			size:      1,
			expected:  blockSize4k,
		},
		{
			blockSize: blockSize4k,
			size:      blockSize4k - 1,
			expected:  blockSize4k,
		},
		{
			blockSize: blockSize4k,
			size:      blockSize4k,
			expected:  blockSize4k,
		},
		{
			blockSize: blockSize4k,
			size:      blockSize4k + 1,
			expected:  2 * blockSize4k,
		},
		{
			blockSize: blockSize4k,
			size:      2*blockSize4k - 1,
			expected:  2 * blockSize4k,
		},
		{
			blockSize: blockSize4k,
			size:      2 * blockSize4k,
			expected:  2 * blockSize4k,
		},
		{
			blockSize: blockSize4k,
			size:      2*blockSize4k + 1,
			expected:  3 * blockSize4k,
		},
		{
			blockSize: blockSize4k,
			size:      maxint64 - blockSize4k - 1,
			expected:  maxint64 - blockSize4k + 1,
		},
		{
			blockSize: blockSize4k,
			size:      maxint64 - blockSize4k,
			expected:  maxint64 - blockSize4k + 1,
		},
		{
			blockSize: blockSize4k,
			size:      maxint64 - blockSize4k + 1,
			expected:  maxint64 - blockSize4k + 1,
		},
		{
			blockSize: maxint64,
			size:      maxint64,
			expected:  maxint64,
		},
		{
			blockSize: maxint64,
			size:      maxint64 - 1,
			expected:  maxint64,
		},
		{
			blockSize: maxint64 - 1,
			size:      maxint64 - 1,
			expected:  maxint64 - 1,
		},
		{
			blockSize: 2,
			size:      maxint64 - 1,
			expected:  maxint64 - 1,
		},
		{
			blockSize: 2,
			size:      maxint64 - 2,
			expected:  maxint64 - 1,
		},
		{
			blockSize: 1,
			size:      maxint64 - 1,
			expected:  maxint64 - 1,
		},
		{
			blockSize: 1,
			size:      maxint64 - 2,
			expected:  maxint64 - 2,
		},
		{
			blockSize: 1,
			size:      maxint64,
			expected:  maxint64,
		},
		{
			blockSize: 1024,
			size:      maxint64 - 1024 + 1,
			expected:  maxint64 - 1024 + 1,
		},
		{
			blockSize: 512,
			size:      maxint64 - 512 + 1,
			expected:  maxint64 - 512 + 1,
		},
		{
			blockSize: 1000,
			size:      (maxint64/1000)*1000 - 1,
			expected:  (maxint64 / 1000) * 1000,
		},
		{
			blockSize: 1000,
			size:      (maxint64 / 1000) * 1000,
			expected:  (maxint64 / 1000) * 1000,
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("blockSize=%d-size=%d", tc.blockSize, tc.size), func(t *testing.T) {
			s, err := stat.GetBlockAlignedSize(tc.size, tc.blockSize)

			require.NoError(t, err)
			require.Equal(t, tc.expected, s)
		})
	}
}

func TestGetBlockAlignedSizeErrors(t *testing.T) {
	cases := []struct {
		blockSize int64
		size      int64
	}{
		{
			blockSize: 0,
			size:      1,
		},
		{
			blockSize: -1,
			size:      1,
		},
		{
			blockSize: -2,
			size:      1,
		},
		{
			blockSize: 256,
			size:      -1,
		},
		{
			blockSize: 256,
			size:      maxint64 - 256 + 2,
		},
		{
			blockSize: 2,
			size:      maxint64,
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("blockSize=%d-size=%d", tc.blockSize, tc.size), func(t *testing.T) {
			s, err := stat.GetBlockAlignedSize(tc.size, tc.blockSize)

			require.Error(t, err)
			require.EqualValues(t, -1, s)
		})
	}
}
