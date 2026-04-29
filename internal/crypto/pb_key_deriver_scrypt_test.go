package crypto_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/crypto"
)

func TestNewScryptKeyDeriverWithMemory(t *testing.T) {
	t.Run("creates custom algorithm with specified memory", func(t *testing.T) {
		algoName := crypto.NewScryptKeyDeriverWithMemory(64)

		require.Equal(t, "scrypt-65536-8-1", algoName)

		// Verify the algorithm is registered and works
		salt := []byte("0123456789012345")
		key, err := crypto.DeriveKeyFromPassword("testpassword", salt, 32, algoName)
		require.NoError(t, err)
		require.Len(t, key, 32)
	})

	t.Run("creates different algorithms for different memory", func(t *testing.T) {
		algo128 := crypto.NewScryptKeyDeriverWithMemory(128)
		algo32 := crypto.NewScryptKeyDeriverWithMemory(32)

		require.NotEqual(t, algo128, algo32)

		salt := []byte("0123456789012345")
		key128, err := crypto.DeriveKeyFromPassword("testpassword", salt, 32, algo128)
		require.NoError(t, err)

		key32, err := crypto.DeriveKeyFromPassword("testpassword", salt, 32, algo32)
		require.NoError(t, err)

		require.NotEqual(t, key128, key32)
	})

	t.Run("same memory produces same key", func(t *testing.T) {
		algoName := crypto.NewScryptKeyDeriverWithMemory(128)
		salt := []byte("0123456789012345")

		key1, err := crypto.DeriveKeyFromPassword("testpassword", salt, 32, algoName)
		require.NoError(t, err)

		key2, err := crypto.DeriveKeyFromPassword("testpassword", salt, 32, algoName)
		require.NoError(t, err)

		require.Equal(t, key1, key2)
	})

	t.Run("calculates N correctly for various memory sizes", func(t *testing.T) {
		algo32 := crypto.NewScryptKeyDeriverWithMemory(32)
		require.Equal(t, "scrypt-32768-8-1", algo32)

		algo256 := crypto.NewScryptKeyDeriverWithMemory(256)
		require.Equal(t, "scrypt-262144-8-1", algo256)
	})
}

func TestDefaultScryptAlgorithmStillWorks(t *testing.T) {
	salt := []byte("0123456789012345")
	key, err := crypto.DeriveKeyFromPassword("testpassword", salt, 32, crypto.ScryptAlgorithm)
	require.NoError(t, err)
	require.Len(t, key, 32)
}