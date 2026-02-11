//go:build !386 && !arm && !mips && !mipsle

package bigmap

// On 64-bit architectures, use default memory parameters optimized for
// large address spaces.
const (
	defaultNumMemorySegments    = 8                  // number of segments to keep in RAM
	defaultMemorySegmentSize    = int64(18000000)    // 18MB enough to store >1M 16-17-byte keys
	defaultFileSegmentSize      = 1024 << 20         // 1 GiB
	defaultInitialSizeLogarithm = 20
)
