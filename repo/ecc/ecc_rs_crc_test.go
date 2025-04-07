package ecc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/encryption"
)

func Test_RsCrc32_AssertSizeAlwaysGrow(t *testing.T) {
	t.Skip("Only needed to run once the size algo changes because this is slow")

	impl, err := newReedSolomonCrcECC(&Options{
		Algorithm:       AlgorithmReedSolomonWithCrc32,
		OverheadPercent: 2,
	})
	require.NoError(t, err)

	last := 0

	for i := 1; i < 10*1024*1024; i++ {
		sizes := impl.computeSizesFromOriginal(i)
		total := computeFinalFileSize(&sizes, i)

		//nolint:gocritic
		// println(fmt.Sprintf("%-8v -> b:%-4v s:%-8v t:%-8v", i, sizes.Blocks, sizes.ShardSize, total))

		if sizes.StorePadding {
			require.GreaterOrEqual(t, total, last)
		} else {
			require.Greater(t, total, last)
		}

		sizes2 := impl.computeSizesFromStored(total)

		require.Equal(t, sizes.Blocks, sizes2.Blocks)
		require.Equal(t, sizes.ShardSize, sizes2.ShardSize)
		require.Equal(t, sizes.DataShards, sizes2.DataShards)
		require.Equal(t, sizes.ParityShards, sizes2.ParityShards)
		require.Equal(t, sizes.StorePadding, sizes2.StorePadding)

		last = total
	}
}

func Test_RsCrc32_2p_1b(t *testing.T) {
	t.Parallel()

	opts := &Options{
		Algorithm:       AlgorithmReedSolomonWithCrc32,
		OverheadPercent: 2,
		MaxShardSize:    1024,
	}

	originalSize := 1
	eccSize := 39
	testRsCrc32NoChange(t, opts, originalSize, eccSize)
	testRsCrc32ChangeInData(t, opts, originalSize, 1, eccSize, true)
	testRsCrc32ChangeInDataCrc(t, opts, originalSize, 1, eccSize, true)
	testRsCrc32ChangeInParity(t, opts, originalSize, 2, eccSize, true)
	testRsCrc32ChangeInParityCrc(t, opts, originalSize, 2, eccSize, true)
}

func Test_RsCrc32_2p_10kb(t *testing.T) {
	t.Parallel()

	opts := &Options{
		Algorithm:       AlgorithmReedSolomonWithCrc32,
		OverheadPercent: 2,
		MaxShardSize:    1024,
	}

	originalSize := 10 * 1024
	eccSize := 810
	testRsCrc32NoChange(t, opts, originalSize, eccSize)
	testRsCrc32ChangeInData(t, opts, originalSize, 2, eccSize, true)
	testRsCrc32ChangeInData(t, opts, originalSize, 3, eccSize, false)
	testRsCrc32ChangeInDataCrc(t, opts, originalSize, 2, eccSize, true)
	testRsCrc32ChangeInDataCrc(t, opts, originalSize, 3, eccSize, false)
	testRsCrc32ChangeInParity(t, opts, originalSize, 2, eccSize, true)
	testRsCrc32ChangeInParityCrc(t, opts, originalSize, 2, eccSize, true)
}

func Test_RsCrc32_10p_1mb(t *testing.T) {
	t.Parallel()

	opts := &Options{
		Algorithm:       AlgorithmReedSolomonWithCrc32,
		OverheadPercent: 10,
		MaxShardSize:    1024,
	}

	originalSize := 1024 * 1024
	eccSize := 115128
	testRsCrc32NoChange(t, opts, originalSize, eccSize)
	testRsCrc32ChangeInData(t, opts, originalSize, 12, eccSize, true)
	testRsCrc32ChangeInData(t, opts, originalSize, 13, eccSize, false)
	testRsCrc32ChangeInDataCrc(t, opts, originalSize, 12, eccSize, true)
	testRsCrc32ChangeInDataCrc(t, opts, originalSize, 13, eccSize, false)
	testRsCrc32ChangeInParity(t, opts, originalSize, 12, eccSize, true)
	testRsCrc32ChangeInParityCrc(t, opts, originalSize, 12, eccSize, true)
}

func testRsCrc32NoChange(t *testing.T, opts *Options, originalSize, expectedEccSize int) {
	t.Helper()

	testPutAndGet(t, opts, originalSize, expectedEccSize, true,
		func(impl encryption.Encryptor, data []byte) {})
}

func testRsCrc32ChangeInData(t *testing.T, opts *Options, originalSize, changedBytes, expectedEccSize int, expectedSuccess bool) {
	t.Helper()

	testPutAndGet(t, opts, originalSize, expectedEccSize, expectedSuccess,
		func(impl encryption.Encryptor, data []byte) {
			sizes := impl.(*ReedSolomonCrcECC).computeSizesFromOriginal(originalSize)
			parity := sizes.ParityShards * (crcSize + sizes.ShardSize) * sizes.Blocks

			for i := range changedBytes {
				flipByte(data, parity+i*(crcSize+sizes.ShardSize)+crcSize)
			}
		})
}

func testRsCrc32ChangeInDataCrc(t *testing.T, opts *Options, originalSize, changedBytes, expectedEccSize int, expectedSuccess bool) {
	t.Helper()

	testPutAndGet(t, opts, originalSize, expectedEccSize, expectedSuccess,
		func(impl encryption.Encryptor, data []byte) {
			sizes := impl.(*ReedSolomonCrcECC).computeSizesFromOriginal(originalSize)
			parity := sizes.ParityShards * (crcSize + sizes.ShardSize) * sizes.Blocks

			for i := range changedBytes {
				flipByte(data, parity+i*(crcSize+sizes.ShardSize))
			}
		})
}

func testRsCrc32ChangeInParity(t *testing.T, opts *Options, originalSize, changedBytes, expectedEccSize int, expectedSuccess bool) {
	t.Helper()

	testPutAndGet(t, opts, originalSize, expectedEccSize, expectedSuccess,
		func(impl encryption.Encryptor, data []byte) {
			sizes := impl.(*ReedSolomonCrcECC).computeSizesFromOriginal(originalSize)

			for i := range changedBytes {
				flipByte(data, i*(crcSize+sizes.ShardSize)+crcSize)
			}
		})
}

func testRsCrc32ChangeInParityCrc(t *testing.T, opts *Options, originalSize, changedBytes, expectedEccSize int, expectedSuccess bool) {
	t.Helper()

	testPutAndGet(t, opts, originalSize, expectedEccSize, expectedSuccess,
		func(impl encryption.Encryptor, data []byte) {
			sizes := impl.(*ReedSolomonCrcECC).computeSizesFromOriginal(originalSize)

			for i := range changedBytes {
				flipByte(data, i*(crcSize+sizes.ShardSize))
			}
		})
}

func computeFinalFileSize(s *sizesInfo, size int) int {
	if s.StorePadding {
		return computeFinalFileSizeWithPadding(s.DataShards, s.ParityShards, s.ShardSize, s.Blocks)
	}

	return computeFinalFileSizeWithoutPadding(size, s.ParityShards, s.ShardSize, s.Blocks)
}

func computeFinalFileSizeWithoutPadding(inputSize, parityShards, shardSize, blocks int) int {
	sizePlusLength := lengthSize + inputSize
	return parityShards*(crcSize+shardSize)*blocks + sizePlusLength + ceilInt(sizePlusLength, shardSize)*crcSize
}
