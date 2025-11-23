package crypto_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/crypto"
)

func TestDeriveKeyFromMasterKey(t *testing.T) {
	const testPurpose = "the-test-purpose"

	var (
		testMasterKey = []byte("ABCDEFGHIJKLMNOP")
		testSalt      = []byte("0123456789012345")
	)

	t.Run("ReturnsKey", func(t *testing.T) {
		key, err := crypto.DeriveKeyFromMasterKey(testMasterKey, testSalt, testPurpose, 32)
		require.NoError(t, err)

		expected := "828769ee8969bc37f11dbaa32838f8db6c19daa6e3ae5f5eed2da2d94d8faddb"
		got := fmt.Sprintf("%02x", key)
		require.Equal(t, expected, got)
	})

	t.Run("ErrorOnNilMasterKey", func(t *testing.T) {
		k, err := crypto.DeriveKeyFromMasterKey(nil, testSalt, testPurpose, 32)
		require.Error(t, err)
		require.Nil(t, k)
	})

	t.Run("ErrorOnEmptyMasterKey", func(t *testing.T) {
		k, err := crypto.DeriveKeyFromMasterKey([]byte{}, testSalt, testPurpose, 32)
		require.Error(t, err)
		require.Nil(t, k)
	})
}
