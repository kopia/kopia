package content

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/transform"
)

type failingEncryptor struct {
	transform.Transformer
	err error
}

func (f failingEncryptor) ToRepository(input gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	return f.err
}

func TestEncryptedBlobManager(t *testing.T) {
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	fs := blobtesting.NewFaultyStorage(st)
	f := &format.ContentFormat{
		Hash:       hashing.DefaultAlgorithm,
		Encryption: encryption.DefaultAlgorithm,
	}
	hf, err := hashing.CreateHashFunc(f)
	require.NoError(t, err)
	enc, err := encryption.CreateEncryptor(f)
	require.NoError(t, err)

	ebm := transformedBlobMgr{
		st:             fs,
		t:              staticCrypter{hf, enc},
		indexBlobCache: nil,
		log:            logging.NullLogger,
	}

	ctx := testlogging.Context(t)

	bm, err := ebm.writeBlob(ctx, gather.FromSlice([]byte{1, 2, 3}), "x", "session1")
	require.NoError(t, err)

	stbm, err := st.GetMetadata(ctx, bm.BlobID)
	require.NoError(t, err)

	require.Equal(t, stbm, bm)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	require.NoError(t, ebm.readBlob(ctx, bm.BlobID, &tmp))

	// data corruption
	data[bm.BlobID][0] ^= 1

	require.Error(t, ebm.readBlob(ctx, bm.BlobID, &tmp))

	require.ErrorIs(t, ebm.readBlob(ctx, "no-such-blob", &tmp), blob.ErrBlobNotFound)

	someError := errors.Errorf("some error")

	fs.AddFault(blobtesting.MethodPutBlob).ErrorInstead(someError)

	_, err = ebm.writeBlob(ctx, gather.FromSlice([]byte{1, 2, 3, 4}), "x", "session1")
	require.ErrorIs(t, err, someError)

	someError2 := errors.Errorf("some error 2")

	ebm.t = staticCrypter{hf, failingEncryptor{nil, someError2}}

	_, err = ebm.writeBlob(ctx, gather.FromSlice([]byte{1, 2, 3, 4}), "x", "session1")
	require.ErrorIs(t, err, someError2)
}

type staticCrypter struct {
	h hashing.HashFunc
	e transform.Transformer
}

func (p staticCrypter) Transformer() transform.Transformer {
	return p.e
}

func (p staticCrypter) HashFunc() hashing.HashFunc {
	return p.h
}
