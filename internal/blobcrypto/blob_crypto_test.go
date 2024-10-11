package blobcrypto

import (
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
)

func TestBlobCrypto(t *testing.T) {
	f := &format.ContentFormat{
		Hash:       hashing.DefaultAlgorithm,
		Encryption: encryption.DefaultAlgorithm,
	}
	hf, err := hashing.CreateHashFunc(f)
	require.NoError(t, err)
	enc, err := encryption.CreateEncryptor(f)
	require.NoError(t, err)

	cr := StaticCrypter{hf, enc}

	var tmp, tmp2, tmp3 gather.WriteBuffer
	defer tmp.Close()
	defer tmp2.Close()
	defer tmp3.Close()

	id, err := Encrypt(cr, gather.FromSlice([]byte{1, 2, 3}), "n", "mysessionid", &tmp)
	require.NoError(t, err)

	id2, err := Encrypt(cr, gather.FromSlice([]byte{1, 2, 4}), "n", "mysessionid", &tmp2)
	require.NoError(t, err)

	require.NotEqual(t, id, id2)

	require.NoError(t, Decrypt(cr, tmp.Bytes(), id, &tmp3))
	require.Equal(t, []byte{1, 2, 3}, tmp3.ToByteSlice())
	require.NoError(t, Decrypt(cr, tmp2.Bytes(), id2, &tmp3))
	require.Equal(t, []byte{1, 2, 4}, tmp3.ToByteSlice())

	// decrypting using invalid ID fails
	require.Error(t, Decrypt(cr, tmp.Bytes(), id2, &tmp3))
	require.Error(t, Decrypt(cr, tmp2.Bytes(), id, &tmp3))

	require.True(t, strings.HasPrefix(string(id), "n"))
	require.True(t, strings.HasSuffix(string(id), "-mysessionid"), id)

	// negative cases
	require.Error(t, Decrypt(cr, tmp.Bytes(), "invalid-blob-id", &tmp3))
	require.Error(t, Decrypt(cr, tmp.Bytes(), "zzz0123456789abcdef0123456789abcde-suffix", &tmp3))
	require.Error(t, Decrypt(cr, tmp.Bytes(), id2, &tmp3))
	require.Error(t, Decrypt(cr, gather.FromSlice([]byte{2, 3, 4}), id, &tmp2))
}

type badEncryptor struct{}

func (badEncryptor) Encrypt(input gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	return errors.New("some error")
}

func (badEncryptor) Decrypt(input gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	return errors.New("some error")
}

func (badEncryptor) Overhead() int { return 0 }

func TestBlobCrypto_Invalid(t *testing.T) {
	cr := StaticCrypter{
		func(output []byte, data gather.Bytes) []byte {
			// invalid hash
			return append(output, 9, 9, 9, 9)
		},
		badEncryptor{},
	}

	var tmp, tmp2, tmp3 gather.WriteBuffer
	defer tmp.Close()
	defer tmp2.Close()
	defer tmp3.Close()

	_, err := Encrypt(cr, gather.FromSlice([]byte{1, 2, 3}), "n", "mysessionid", &tmp)
	require.Error(t, err)

	f := &format.ContentFormat{
		Hash:       hashing.DefaultAlgorithm,
		Encryption: encryption.DefaultAlgorithm,
	}

	// now fix HashFunction but encryption still fails.
	hf, err := hashing.CreateHashFunc(f)
	require.NoError(t, err)

	cr.Hash = hf

	_, err = Encrypt(cr, gather.FromSlice([]byte{1, 2, 3}), "n", "mysessionid", &tmp)
	require.Error(t, err)
}
