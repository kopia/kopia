//go:build !386 && !arm && !mips && !mipsle

package bigmap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultConstants64Bit(t *testing.T) {
	// Verify that 64-bit architectures use standard memory parameters
	require.Equal(t, 8, defaultNumMemorySegments, "64-bit should use 8 memory segments")
	require.Equal(t, int64(18*1000000), defaultMemorySegmentSize, "64-bit should use 18MB segments")
	require.Equal(t, 1024<<20, defaultFileSegmentSize, "64-bit should use 1GB file segments")
	require.Equal(t, 20, defaultInitialSizeLogarithm, "64-bit should use initial size 2^20")
}
