//go:build 386 || arm || mips || mipsle

package crypto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScryptParameters32Bit(t *testing.T) {
	// Verify that 32-bit architectures use reduced scrypt parameters
	// to avoid running out of address space during key derivation
	require.Equal(t, 16384, scryptCostParameterN, "32-bit should use N=16384")
	require.Equal(t, 4, scryptCostParameterR, "32-bit should use r=4")
	require.Equal(t, 1, scryptCostParameterP, "32-bit should use p=1")

	// Verify both algorithm names are registered
	require.Equal(t, "scrypt-65536-8-1", ScryptAlgorithm, "ScryptAlgorithm constant should remain unchanged for compatibility")
	require.Equal(t, "scrypt-16384-4-1", Scrypt32BitAlgorithm, "32-bit algorithm name should be scrypt-16384-4-1")

	// Memory usage: 128 * N * r * p = 128 * 16384 * 4 * 1 = 8,388,608 bytes (~8MB)
	// This is 8x less than the 64-bit default and should work on 32-bit systems
	expectedMemory := 128 * scryptCostParameterN * scryptCostParameterR * scryptCostParameterP
	require.Equal(t, 8388608, expectedMemory, "32-bit should use ~8MB memory for scrypt")

	// Verify that both algorithm names work (they point to the same implementation)
	salt := make([]byte, 16)
	password := "test-password"

	key1, err1 := DeriveKeyFromPassword(password, salt, 32, ScryptAlgorithm)
	require.NoError(t, err1, "Standard scrypt algorithm should be registered and work")
	require.Len(t, key1, 32, "Should derive 32-byte key")

	key2, err2 := DeriveKeyFromPassword(password, salt, 32, Scrypt32BitAlgorithm)
	require.NoError(t, err2, "32-bit scrypt algorithm should be registered and work")
	require.Len(t, key2, 32, "Should derive 32-byte key")

	// Both algorithm names should produce the same result (since they use the same implementation)
	require.Equal(t, key1, key2, "Both algorithm names should produce identical keys")
}
