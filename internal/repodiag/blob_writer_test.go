package repodiag_test

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobcrypto"
	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/repodiag"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
)

func TestDiagWriter(t *testing.T) {
	d := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(d, nil, nil)
	fs := blobtesting.NewFaultyStorage(st)

	w := repodiag.NewWriter(fs, newStaticCrypter(t))
	ctx := testlogging.Context(t)
	closeCalled1 := make(chan struct{})
	closeCalled2 := make(chan struct{})

	w.EncryptAndWriteBlobAsync(ctx, "prefix1_", gather.FromSlice([]byte{1, 2, 3}), func() {
		close(closeCalled1)
	})

	w.EncryptAndWriteBlobAsync(ctx, "prefix2_", gather.FromSlice([]byte{2, 3, 4}), func() {
		close(closeCalled2)
	})

	<-closeCalled1
	<-closeCalled2

	// simulate write failure
	someErr := errors.New("some error")
	fs.AddFault(blobtesting.MethodPutBlob).ErrorInstead(someErr)

	closeCalled3 := make(chan struct{})

	w.EncryptAndWriteBlobAsync(ctx, "prefix3_", gather.FromSlice([]byte{1}), func() {
		close(closeCalled3)
	})

	<-closeCalled3

	// blob IDs are deterministic based on content
	require.Len(t, d, 2)
	require.Contains(t, d, blob.ID("prefix1_11c0e79b71c3976ccd0c02d1310e2516"))
	require.Contains(t, d, blob.ID("prefix2_24ff687b6ca564bd005a99420c90a0db"))

	t0 := clock.Now()

	w.EncryptAndWriteBlobAsync(ctx, "prefix4_", gather.FromSlice([]byte{3, 4, 5}), func() {
		time.Sleep(1100 * time.Millisecond)
	})

	// make sure close waits for all async writes to complete
	w.Wait(ctx)

	require.Greater(t, clock.Now().Sub(t0), time.Second)
}

func newStaticCrypter(t *testing.T) blobcrypto.Crypter {
	t.Helper()

	p := &format.ContentFormat{
		Encryption: encryption.DefaultAlgorithm,
		Hash:       hashing.DefaultAlgorithm,
	}

	enc, err := encryption.CreateEncryptor(p)
	if err != nil {
		t.Fatalf("unable to create encryptor: %v", err)
	}

	hf, err := hashing.CreateHashFunc(p)
	if err != nil {
		t.Fatalf("unable to create hash: %v", err)
	}

	return blobcrypto.StaticCrypter{
		Hash:       hf,
		Encryption: enc,
	}
}
