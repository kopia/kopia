package ecc

import (
	"testing"

	"github.com/kopia/kopia/repo/encryption"
	"github.com/stretchr/testify/require"
)

/*
func Test_RsCrc32_FileGrowthByShards(t *testing.T) {
	var buffer gather.WriteBuffer
	defer buffer.Close()

	sizes := []int{1, 10, 100, 1024, 10 * 1024, 100 * 1024, 1024 * 1024, 10 * 1024 * 1024}

	print(fmt.Sprintf("%-5v   ", "Shard"))
	print(fmt.Sprintf("%-9v   ", "T.Parity"))
	print(fmt.Sprintf("%-9v   ", "T.Shards"))
	for _, size := range sizes {
		print(fmt.Sprintf("%9v%6v%%   ", units.BytesStringBase2(int64(size)), ""))
	}
	println("")

	for _, shard := range []int{32, 64, 128, 256, 512, 1024} {
		impl, err := newReedSolomonCrcECC(&Options{
			Algorithm:     AlgorithmReedSolomonWithCrc32,
			MaxShardSize:  shard,
			SpaceOverhead: 2,
		})
		require.NoError(t, err)

		print(fmt.Sprintf("%-5v   ", shard))
		print(fmt.Sprintf("%9v   ", units.BytesStringBase2(int64(impl.ThresholdParityInput))))
		print(fmt.Sprintf("%9v   ", units.BytesStringBase2(int64(impl.ThresholdBlocksInput))))

		for _, size := range sizes {
			data := make([]byte, size)
			for i := 0; i < size; i++ {
				data[i] = byte(i%255 + 1)
			}

			buffer.Reset()
			err = impl.Encrypt(gather.FromSlice(data), nil, &buffer)
			require.NoError(t, err)

			encLen := buffer.Length()
			growth := int(math.Round(100 * (float64(encLen)/float64(size) - 1)))
			print(fmt.Sprintf("%9v%6v%%   ", units.BytesStringBase2(int64(encLen)), growth))
		}

		println("")
	}
}
*/

/*
func Test_RsCrc32_FileGrowthBySpaceOverhead(t *testing.T) {
	var buffer gather.WriteBuffer
	defer buffer.Close()

	sizes := []int{1, 10, 100, 1024, 10 * 1024, 100 * 1024, 1024 * 1024, 10 * 1024 * 1024}

	print(fmt.Sprintf("%-4v   ", "S.O."))
	for _, size := range sizes {
		print(fmt.Sprintf("%9v%5v%%   ", units.BytesStringBase2(int64(size)), ""))
	}
	println("")

	for _, overhead := range []uint8{1, 2, 5, 10, 20} {
		impl, err := newReedSolomonCrcECC(&Options{
			Algorithm:     AlgorithmReedSolomonWithCrc32,
			SpaceOverhead: overhead,
		})
		require.NoError(t, err)

		print(fmt.Sprintf("%-3v%%   ", overhead))

		for _, size := range sizes {
			data := make([]byte, size)
			for i := 0; i < size; i++ {
				data[i] = byte(i%255 + 1)
			}

			buffer.Reset()
			err = impl.Encrypt(gather.FromSlice(data), nil, &buffer)
			require.NoError(t, err)

			encLen := buffer.Length()
			growth := int(math.Round(100 * (float64(encLen)/float64(size) - 1)))
			print(fmt.Sprintf("%9v%5v%%   ", units.BytesStringBase2(int64(encLen)), growth))
		}

		println("")
	}
}
*/

func Test_RsCrc32_AssertSizeAlwaysGrow(t *testing.T) {
	t.Skip("Only needed to run once the size algo changes because this is slow")

	impl, err := newReedSolomonCrcECC(&Options{
		Algorithm:     AlgorithmReedSolomonWithCrc32,
		SpaceOverhead: 2,
	})
	require.NoError(t, err)

	last := 0

	for i := 1; i < 10*1024*1024; i++ {
		sizes := impl.computeSizesFromOriginal(i)
		total := sizes.computeFinalFileSize(i)

		//nolint:gocritic
		// println(fmt.Sprintf("%-8v -> b:%-4v s:%-8v t:%-8v", i, sizes.Blocks, sizes.ShardSize, total))

		if sizes.StorePadding {
			require.True(t, total >= last)
		} else {
			require.True(t, total > last)
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
		Algorithm:     AlgorithmReedSolomonWithCrc32,
		SpaceOverhead: 2,
		MaxShardSize:  1024,
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
		Algorithm:     AlgorithmReedSolomonWithCrc32,
		SpaceOverhead: 2,
		MaxShardSize:  1024,
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
		Algorithm:     AlgorithmReedSolomonWithCrc32,
		SpaceOverhead: 10,
		MaxShardSize:  1024,
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

			for i := 0; i < changedBytes; i++ {
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

			for i := 0; i < changedBytes; i++ {
				flipByte(data, parity+i*(crcSize+sizes.ShardSize))
			}
		})
}

func testRsCrc32ChangeInParity(t *testing.T, opts *Options, originalSize, changedBytes, expectedEccSize int, expectedSuccess bool) {
	t.Helper()

	testPutAndGet(t, opts, originalSize, expectedEccSize, expectedSuccess,
		func(impl encryption.Encryptor, data []byte) {
			sizes := impl.(*ReedSolomonCrcECC).computeSizesFromOriginal(originalSize)

			for i := 0; i < changedBytes; i++ {
				flipByte(data, i*(crcSize+sizes.ShardSize)+crcSize)
			}
		})
}

func testRsCrc32ChangeInParityCrc(t *testing.T, opts *Options, originalSize, changedBytes, expectedEccSize int, expectedSuccess bool) {
	t.Helper()

	testPutAndGet(t, opts, originalSize, expectedEccSize, expectedSuccess,
		func(impl encryption.Encryptor, data []byte) {
			sizes := impl.(*ReedSolomonCrcECC).computeSizesFromOriginal(originalSize)

			for i := 0; i < changedBytes; i++ {
				flipByte(data, i*(crcSize+sizes.ShardSize))
			}
		})
}
