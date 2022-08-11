package encryption_test

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/encryption"
)

func TestNoEncryptors(t *testing.T) {
	t.Parallel()

	require.Equal(t, nil, encryption.Pipeline())
}

func TestOnlyNil(t *testing.T) {
	t.Parallel()

	require.Equal(t, nil, encryption.Pipeline(nil, nil))
}

func TestOne(t *testing.T) {
	t.Parallel()

	e := &appendEncryptor{}

	require.Equal(t, e, encryption.Pipeline(e))
}

func TestOneAndNil(t *testing.T) {
	t.Parallel()

	e := &appendEncryptor{}

	require.Equal(t, e, encryption.Pipeline(nil, e, nil))
}

func TestTwo(t *testing.T) {
	t.Parallel()

	e1 := &appendEncryptor{Data: 1}
	e2 := &appendEncryptor{Data: 2}

	ep := encryption.Pipeline(e1, e2)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	err := ep.Encrypt(gather.FromSlice([]byte{}), nil, &tmp)
	require.NoError(t, err)

	bytes := tmp.ToByteSlice()
	require.Equal(t, 2, len(bytes))
	require.Equal(t, byte(1), bytes[0])
	require.Equal(t, byte(2), bytes[1])

	tmp.Reset()

	err = ep.Decrypt(gather.FromSlice(bytes), nil, &tmp)
	require.NoError(t, err)

	bytes = tmp.ToByteSlice()
	require.Equal(t, 0, len(bytes))
}

type appendEncryptor struct {
	Data byte
}

func (a appendEncryptor) Encrypt(plainText gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	output.Write(plainText.ToByteSlice())
	output.Write([]byte{a.Data})
	return nil
}

func (a appendEncryptor) Decrypt(cipherText gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	inputBytes := cipherText.ToByteSlice()
	inputLen := len(inputBytes)

	if inputLen < 1 || inputBytes[inputLen-1] != a.Data {
		return errors.New("Invalid input")
	}

	output.Write(inputBytes[:inputLen-1])
	return nil
}

func (a appendEncryptor) Overhead() int {
	panic("Not used")
}
