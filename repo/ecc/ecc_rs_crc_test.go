package ecc_test

import (
	"github.com/kopia/kopia/repo/ecc"
	"github.com/kopia/kopia/repo/encryption"
	"testing"
)

/*
func Test_RsCrc32_ShardSizes(t *testing.T) {
	impl, err := ecc.NewRsCrcEcc(&ecc.Options{
		SpaceUsedPercentage: 2,
	})
	require.NoError(t, err)

	last := 0

	for i := 1; i < 40*1024*1024; i++ {
		blocks, crcSize, shardSize, originalSize := impl.ComputeSizesFromOriginal(i)
		total := impl.ParityShards*(crcSize+shardSize)*blocks + originalSize + ecc.CeilInt(originalSize, shardSize)*crcSize

		blocks2, crcSize2, shardSize2, originalSize2 := impl.ComputeSizesFromStored(total)

		//println(fmt.Sprintf("%-8v -> b:%-8v t:%-8v", i, blocks, total))

		require.True(t, total > last)
		require.Equal(t, blocks, blocks2)
		require.Equal(t, crcSize, crcSize2)
		require.Equal(t, shardSize, shardSize2)
		require.Equal(t, originalSize, originalSize2)

		last = total
	}
}
*/

func Test_RsCrc32_2p_1b(t *testing.T) {
	t.Parallel()

	opts := &ecc.Options{
		Algorithm: ecc.RsCrc322pEccName,
	}

	originalSize := 1
	eccSize := 2060
	testRsCrc32NoChange(t, opts, originalSize, eccSize)
	testRsCrc32ChangeInData(t, opts, originalSize, 1, eccSize, true)
	testRsCrc32ChangeInDataCrc(t, opts, originalSize, 1, eccSize, true)
	testRsCrc32ChangeInParity(t, opts, originalSize, 2, eccSize, true)
	testRsCrc32ChangeInParityCrc(t, opts, originalSize, 2, eccSize, true)
}

func Test_RsCrc32_2p_10kb(t *testing.T) {
	t.Parallel()

	opts := &ecc.Options{
		Algorithm: ecc.RsCrc322pEccName,
	}

	originalSize := 10 * 1024
	eccSize := 2096
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

	opts := &ecc.Options{
		Algorithm: ecc.RsCrc3210pEccName,
	}

	originalSize := 1024 * 1024
	eccSize := 116224
	testRsCrc32NoChange(t, opts, originalSize, eccSize)
	testRsCrc32ChangeInData(t, opts, originalSize, 12, eccSize, true)
	testRsCrc32ChangeInData(t, opts, originalSize, 13, eccSize, false)
	testRsCrc32ChangeInDataCrc(t, opts, originalSize, 12, eccSize, true)
	testRsCrc32ChangeInDataCrc(t, opts, originalSize, 13, eccSize, false)
	testRsCrc32ChangeInParity(t, opts, originalSize, 12, eccSize, true)
	testRsCrc32ChangeInParityCrc(t, opts, originalSize, 12, eccSize, true)
}

func testRsCrc32NoChange(t *testing.T, opts *ecc.Options, originalSize, expectedEccSize int) {
	testPutAndGet(t, opts, originalSize, expectedEccSize, true,
		func(impl encryption.Encryptor, data []byte) {})
}

func testRsCrc32ChangeInData(t *testing.T, opts *ecc.Options, originalSize, changedBytes, expectedEccSize int, expectedSuccess bool) {
	testPutAndGet(t, opts, originalSize, expectedEccSize, expectedSuccess,
		func(impl encryption.Encryptor, data []byte) {
			blocks, crcSize, shardSize, _ := impl.(*ecc.RsCrcEcc).ComputeSizesFromOriginal(originalSize)
			parity := impl.(*ecc.RsCrcEcc).ParityShards * (crcSize + shardSize) * blocks

			for i := 0; i < changedBytes; i++ {
				flipByte(data, parity+i*(crcSize+shardSize)+crcSize)
			}
		})
}

func testRsCrc32ChangeInDataCrc(t *testing.T, opts *ecc.Options, originalSize, changedBytes, expectedEccSize int, expectedSuccess bool) {
	testPutAndGet(t, opts, originalSize, expectedEccSize, expectedSuccess,
		func(impl encryption.Encryptor, data []byte) {
			blocks, crcSize, shardSize, _ := impl.(*ecc.RsCrcEcc).ComputeSizesFromOriginal(originalSize)
			parity := impl.(*ecc.RsCrcEcc).ParityShards * (crcSize + shardSize) * blocks

			for i := 0; i < changedBytes; i++ {
				flipByte(data, parity+i*(crcSize+shardSize))
			}
		})
}

func testRsCrc32ChangeInParity(t *testing.T, opts *ecc.Options, originalSize, changedBytes, expectedEccSize int, expectedSuccess bool) {
	testPutAndGet(t, opts, originalSize, expectedEccSize, expectedSuccess,
		func(impl encryption.Encryptor, data []byte) {
			_, crcSize, shardSize, _ := impl.(*ecc.RsCrcEcc).ComputeSizesFromOriginal(originalSize)

			for i := 0; i < changedBytes; i++ {
				flipByte(data, i*(crcSize+shardSize)+crcSize)
			}
		})
}

func testRsCrc32ChangeInParityCrc(t *testing.T, opts *ecc.Options, originalSize, changedBytes, expectedEccSize int, expectedSuccess bool) {
	testPutAndGet(t, opts, originalSize, expectedEccSize, expectedSuccess,
		func(impl encryption.Encryptor, data []byte) {
			_, crcSize, shardSize, _ := impl.(*ecc.RsCrcEcc).ComputeSizesFromOriginal(originalSize)

			for i := 0; i < changedBytes; i++ {
				flipByte(data, i*(crcSize+shardSize))
			}
		})
}
