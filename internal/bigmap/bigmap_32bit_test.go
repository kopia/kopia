//go:build 386 || arm || mips || mipsle

package bigmap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultConstants32Bit(t *testing.T) {
	// Verify that 32-bit architectures use reduced memory parameters
	// to avoid running out of address space during large operations
	require.Equal(t, 4, defaultNumMemorySegments, "32-bit should use 4 memory segments")
	require.Equal(t, int64(4*1000000), defaultMemorySegmentSize, "32-bit should use 4MB segments")
	require.Equal(t, 256<<20, defaultFileSegmentSize, "32-bit should use 256MB file segments")
	require.Equal(t, 18, defaultInitialSizeLogarithm, "32-bit should use initial size 2^18")
}
