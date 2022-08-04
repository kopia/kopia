package ecc_test

import (
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/ecc"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestComputeShares(t *testing.T) {
	t.Parallel()

	testComputeShares(t, 0.1, 254, 2)
	testComputeShares(t, 1, 200, 2)
	testComputeShares(t, 2, 128, 2)
	testComputeShares(t, 10, 128, 12)
}

func testComputeShares(t *testing.T, spaceUsedPercentage float32, expectedRequired, expectedRedundant int) {
	required, redundant := ecc.ComputeShards(spaceUsedPercentage)

	require.Equal(t, expectedRequired, required)
	require.Equal(t, expectedRedundant, redundant)
}

func testPutAndGet(t *testing.T, opts *ecc.Options, originalSize,
	expectedEccSize int, expectedSuccess bool,
	makeChanges func(impl encryption.Encryptor, data []byte)) {

	impl, err := ecc.CreateAlgorithm(opts)
	require.NoError(t, err)

	original := make([]byte, originalSize)
	for i := 0; i < originalSize; i++ {
		original[i] = byte(i%255) + 1
	}

	output := gather.NewWriteBuffer()

	err = impl.Encrypt(gather.FromSlice(original), nil, output)
	require.NoError(t, err)

	result := output.ToByteSlice()
	require.Equal(t, originalSize+expectedEccSize, len(result))

	makeChanges(impl, result)

	output = gather.NewWriteBuffer()

	err = impl.Decrypt(gather.FromSlice(result), nil, output)

	if expectedSuccess {
		require.NoError(t, err)
		require.Equal(t, original, output.ToByteSlice())

	} else {
		require.Error(t, err)
	}
}

func flipByte(data []byte, i int) {
	if data[i] >= 128 {
		data[i] = 0
	} else {
		data[i] = 255
	}
}
