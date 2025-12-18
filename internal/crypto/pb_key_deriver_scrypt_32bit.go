//go:build 386 || arm || mips || mipsle

package crypto

const (
	// On 32-bit architectures, use reduced scrypt parameters to avoid
	// running out of address space. The default parameters (N=65536, r=8)
	// require ~64MB of contiguous memory which can fail on 32-bit systems
	// with fragmented address space.
	//
	// These reduced parameters (N=16384, r=4) require ~8MB of memory,
	// which is 8x less than the default and provides better compatibility
	// with 32-bit address space constraints while maintaining reasonable security.
	scryptCostParameterN = 16384 // reduced from 65536
	scryptCostParameterR = 4     // reduced from 8
	scryptCostParameterP = 1     // unchanged
)
