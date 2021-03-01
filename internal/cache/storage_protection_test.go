package cache_test

import (
	"bytes"
	"testing"

	"github.com/kopia/kopia/internal/cache"
)

func TestHMACStorageProtection(t *testing.T) {
	testStorageProtection(t, cache.ChecksumProtection([]byte{1, 2, 3, 4}))
}

func TestEncryptionStorageProtection(t *testing.T) {
	e, err := cache.AuthenticatedEncryptionProtection([]byte{1})
	if err != nil {
		t.Fatal(err)
	}

	testStorageProtection(t, e)
}

// nolint:thelper
func testStorageProtection(t *testing.T, sp cache.StorageProtection) {
	payload := []byte{0, 1, 2, 3, 4}

	protected := sp.Protect("x", payload)

	unprotected, err := sp.Verify("x", protected)
	if err != nil {
		t.Fatal(err)
	}

	if got, want := unprotected, payload; !bytes.Equal(got, want) {
		t.Fatalf("invalid unprotected payload %x, wanted %x", got, want)
	}

	// flip one bit
	protected[0] ^= 1
}
