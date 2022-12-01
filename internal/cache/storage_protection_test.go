package cache_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/gather"
)

func TestNoStorageProection(t *testing.T) {
	testStorageProtection(t, cache.NoProtection(), false)
}

func TestHMACStorageProtection(t *testing.T) {
	testStorageProtection(t, cache.ChecksumProtection([]byte{1, 2, 3, 4}), true)
}

func TestEncryptionStorageProtection(t *testing.T) {
	e, err := cache.AuthenticatedEncryptionProtection([]byte{1})
	require.NoError(t, err)

	testStorageProtection(t, e, true)
}

//nolint:thelper
func testStorageProtection(t *testing.T, sp cache.StorageProtection, protectsFromBitFlips bool) {
	payload := []byte{0, 1, 2, 3, 4}

	var protected gather.WriteBuffer
	defer protected.Close()

	// append dummy bytes to ensure Reset is called.
	protected.Append([]byte("dummy"))

	sp.Protect("x", gather.FromSlice(payload), &protected)

	var unprotected gather.WriteBuffer
	defer unprotected.Close()

	// append dummy bytes to ensure Reset is called.
	unprotected.Append([]byte("dummy"))

	require.NoError(t, sp.Verify("x", protected.Bytes(), &unprotected))

	if got, want := unprotected.ToByteSlice(), payload; !bytes.Equal(got, want) {
		t.Fatalf("invalid unprotected payload %x, wanted %x", got, want)
	}

	pb := protected.ToByteSlice()

	if protectsFromBitFlips {
		// flip one bit
		pb[0] ^= 1

		require.Error(t, sp.Verify("x", gather.FromSlice(pb), &unprotected))
	} else {
		require.NoError(t, sp.Verify("x", gather.FromSlice(pb), &unprotected))
	}
}
