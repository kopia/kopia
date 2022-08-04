package ecc_test

import (
	"github.com/kopia/kopia/repo/ecc"
	"github.com/kopia/kopia/repo/encryption"
	"testing"
)

func Test_RsBw_Fb_2p_1b(t *testing.T) {
	t.Parallel()

	opts := &ecc.Options{
		Algorithm: ecc.RsBwFb2pEccName,
	}

	originalSize := 1
	eccSize := 32
	testRsBwNoChange(t, opts, originalSize, eccSize)
	testRsBwChangeInOriginal(t, opts, originalSize, 1, eccSize, true)
	testRsBwChangeInEcc(t, opts, originalSize, 1, eccSize, true)
	testRsBwChangeInEcc(t, opts, originalSize, 2, eccSize, false)
}

func Test_RsBw_Fb_2p_10kb(t *testing.T) {
	t.Parallel()

	opts := &ecc.Options{
		Algorithm: ecc.RsBwFb2pEccName,
	}

	originalSize := 10 * 1024
	eccSize := 160
	testRsBwNoChange(t, opts, originalSize, eccSize)
	testRsBwChangeInOriginal(t, opts, originalSize, 1, eccSize, true)
	testRsBwChangeInOriginal(t, opts, originalSize, 2, eccSize, false)
	testRsBwChangeInEcc(t, opts, originalSize, 1, eccSize, true)
	testRsBwChangeInEcc(t, opts, originalSize, 2, eccSize, false)
}

func Test_RsBw_Fb_10p_10kb(t *testing.T) {
	t.Parallel()

	opts := &ecc.Options{
		Algorithm: ecc.RsBwFb10pEccName,
	}

	originalSize := 10 * 1024
	eccSize := 960
	testRsBwNoChange(t, opts, originalSize, eccSize)
	testRsBwChangeInOriginal(t, opts, originalSize, 6, eccSize, true)
	testRsBwChangeInOriginal(t, opts, originalSize, 7, eccSize, false)
	testRsBwChangeInEcc(t, opts, originalSize, 6, eccSize, true)
	testRsBwChangeInEcc(t, opts, originalSize, 7, eccSize, false)
}

func Test_RsBw_Fs_2p_1b(t *testing.T) {
	t.Parallel()

	opts := &ecc.Options{
		Algorithm: ecc.RsBwFs2pEccName,
	}

	originalSize := 1
	eccSize := 2048
	testRsBwNoChange(t, opts, originalSize, eccSize)
	testRsBwChangeInOriginal(t, opts, originalSize, 1, eccSize, true)
	testRsBwChangeInEcc(t, opts, originalSize, 1, eccSize, true)
	testRsBwChangeInEcc(t, opts, originalSize, 2, eccSize, false)
}

func Test_RsBw_Fs_2p_10kb(t *testing.T) {
	t.Parallel()

	opts := &ecc.Options{
		Algorithm: ecc.RsBwFs2pEccName,
	}

	originalSize := 10 * 1024
	eccSize := 2048
	testRsBwNoChange(t, opts, originalSize, eccSize)
	testRsBwChangeInOriginal(t, opts, originalSize, 1, eccSize, true)
	// TODO testRsBwChangeInOriginal(t, opts, originalSize, 10, eccSize, false)
	testRsBwChangeInEcc(t, opts, originalSize, 1, eccSize, true)
	testRsBwChangeInEcc(t, opts, originalSize, 2, eccSize, false)
}

func Test_RsBw_Fs_10p_10kb(t *testing.T) {
	t.Parallel()

	opts := &ecc.Options{
		Algorithm: ecc.RsBwFs10pEccName,
	}

	originalSize := 10 * 1024
	eccSize := 12288
	testRsBwNoChange(t, opts, originalSize, eccSize)
	testRsBwChangeInOriginal(t, opts, originalSize, 6, eccSize, true)
	testRsBwChangeInOriginal(t, opts, originalSize, 7, eccSize, false)
	testRsBwChangeInEcc(t, opts, originalSize, 6, eccSize, true)
	testRsBwChangeInEcc(t, opts, originalSize, 7, eccSize, false)
}

func testRsBwNoChange(t *testing.T, opts *ecc.Options, originalSize, expectedEccSize int) {
	testPutAndGet(t, opts, originalSize, expectedEccSize, true,
		func(impl encryption.Encryptor, data []byte) {})
}

func testRsBwChangeInOriginal(t *testing.T, opts *ecc.Options, originalSize, changedBytes, expectedEccSize int, expectedSuccess bool) {
	testPutAndGet(t, opts, originalSize, expectedEccSize, expectedSuccess,
		func(impl encryption.Encryptor, data []byte) {
			_, shareSize, _ := impl.(*ecc.RsBwEcc).ComputeSizesFromOriginal(originalSize)

			for i := 0; i < changedBytes; i++ {
				flipByte(data, expectedEccSize+(i*shareSize))
			}
		})
}

func testRsBwChangeInEcc(t *testing.T, opts *ecc.Options, originalSize, changedBytes, expectedEccSize int, expectedSuccess bool) {
	testPutAndGet(t, opts, originalSize, expectedEccSize, expectedSuccess,
		func(impl encryption.Encryptor, data []byte) {
			_, shareSize, _ := impl.(*ecc.RsBwEcc).ComputeSizesFromOriginal(originalSize)

			for i := 0; i < changedBytes; i++ {
				flipByte(data, i*shareSize)
			}
		})
}
