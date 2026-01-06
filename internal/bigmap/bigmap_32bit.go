//go:build 386 || arm || mips || mipsle

package bigmap

// On 32-bit architectures, use smaller default memory parameters to avoid
// running out of address space during garbage collection and other operations.
const (
	defaultNumMemorySegments    = 4                 // reduced from 8
	defaultMemorySegmentSize    = int64(4000000)    // 4MB (reduced from 18MB)
	defaultFileSegmentSize      = 256 << 20         // 256 MiB (reduced from 1 GiB)
	defaultInitialSizeLogarithm = 18                // reduced from 20
)
