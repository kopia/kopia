package crypto_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/crypto"
)

func TestNewPBKDF2KeyDeriverWithIterations(t *testing.T) {
	t.Run("creates custom algorithm with specified iterations", func(t *testing.T) {
		const customIterations = 100000
		algoName := crypto.NewPBKDF2KeyDeriverWithIterations(customIterations)

		expectedName := "pbkdf2-sha256-100000"
		require.Equal(t, expectedName, algoName)

		// Verify the algorithm is registered and works
		salt := []byte("0123456789012345")
		key, err := crypto.DeriveKeyFromPassword("testpassword", salt, 32, algoName)
		require.NoError(t, err)
		require.Len(t, key, 32)
	})

	t.Run("creates different algorithms for different iterations", func(t *testing.T) {
		algo1 := crypto.NewPBKDF2KeyDeriverWithIterations(100000)
		algo2 := crypto.NewPBKDF2KeyDeriverWithIterations(200000)

		require.NotEqual(t, algo1, algo2)

		salt := []byte("0123456789012345")
		key1, err := crypto.DeriveKeyFromPassword("testpassword", salt, 32, algo1)
		require.NoError(t, err)

		key2, err := crypto.DeriveKeyFromPassword("testpassword", salt, 32, algo2)
		require.NoError(t, err)

		require.NotEqual(t, key1, key2)
	})

	t.Run("same iterations produce same key", func(t *testing.T) {
		algoName := crypto.NewPBKDF2KeyDeriverWithIterations(150000)
		salt := []byte("0123456789012345")

		key1, err := crypto.DeriveKeyFromPassword("testpassword", salt, 32, algoName)
		require.NoError(t, err)

		key2, err := crypto.DeriveKeyFromPassword("testpassword", salt, 32, algoName)
		require.NoError(t, err)

		require.Equal(t, key1, key2)
	})
}

func TestDefaultPBKDF2AlgorithmStillWorks(t *testing.T) {
	salt := []byte("0123456789012345")
	key, err := crypto.DeriveKeyFromPassword("testpassword", salt, 32, crypto.Pbkdf2Algorithm)
	require.NoError(t, err)
	require.Len(t, key, 32)
}