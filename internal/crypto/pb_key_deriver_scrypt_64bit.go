//go:build !386 && !arm && !mips && !mipsle

package crypto

const (
	// Standard scrypt parameters for 64-bit architectures.
	// These parameters (N=65536, r=8) require ~64MB of memory
	// and provide strong security guarantees.
	scryptCostParameterN = 65536
	scryptCostParameterR = 8
	scryptCostParameterP = 1
)
