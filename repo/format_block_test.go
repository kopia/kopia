package repo

import (
	"context"
	"crypto/sha256"
	"reflect"
	"testing"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/repo/blob"
)

func TestFormatBlockRecovery(t *testing.T) {
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	ctx := context.Background()

	someDataBlock := []byte("aadsdasdas")
	checksummed, err := addFormatBlockChecksumAndLength(someDataBlock)
	if err != nil {
		t.Errorf("error appending checksum: %v", err)
	}
	if got, want := len(checksummed), 2+2+sha256.Size+len(someDataBlock); got != want {
		t.Errorf("unexpected checksummed length: %v, want %v", got, want)
	}

	assertNoError(t, st.PutBlob(ctx, "some-block-by-itself", checksummed))
	assertNoError(t, st.PutBlob(ctx, "some-block-suffix", append(append([]byte(nil), 1, 2, 3), checksummed...)))
	assertNoError(t, st.PutBlob(ctx, "some-block-prefix", append(append([]byte(nil), checksummed...), 1, 2, 3)))

	// mess up checksum
	checksummed[len(checksummed)-3] ^= 1
	assertNoError(t, st.PutBlob(ctx, "bad-checksum", checksummed))
	assertNoError(t, st.PutBlob(ctx, "zero-len", []byte{}))
	assertNoError(t, st.PutBlob(ctx, "one-len", []byte{1}))
	assertNoError(t, st.PutBlob(ctx, "two-len", []byte{1, 2}))
	assertNoError(t, st.PutBlob(ctx, "three-len", []byte{1, 2, 3}))
	assertNoError(t, st.PutBlob(ctx, "four-len", []byte{1, 2, 3, 4}))
	assertNoError(t, st.PutBlob(ctx, "five-len", []byte{1, 2, 3, 4, 5}))

	cases := []struct {
		blobID blob.ID
		err    error
	}{
		{"some-block-by-itself", nil},
		{"some-block-suffix", nil},
		{"some-block-prefix", nil},
		{"bad-checksum", errFormatBlockNotFound},
		{"no-such-block", blob.ErrBlobNotFound},
		{"zero-len", errFormatBlockNotFound},
		{"one-len", errFormatBlockNotFound},
		{"two-len", errFormatBlockNotFound},
		{"three-len", errFormatBlockNotFound},
		{"four-len", errFormatBlockNotFound},
		{"five-len", errFormatBlockNotFound},
	}

	for _, tc := range cases {
		t.Run(string(tc.blobID), func(t *testing.T) {
			v, err := RecoverFormatBlock(ctx, st, tc.blobID, -1)
			if tc.err == nil {
				if !reflect.DeepEqual(v, someDataBlock) || err != nil {
					t.Errorf("unexpected result or error: v=%v err=%v, expected success", v, err)
				}
			} else {
				if v != nil || err != tc.err {
					t.Errorf("unexpected result or error: v=%v err=%v, expected %v", v, err, tc.err)
				}
			}
		})
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("err: %v", err)
	}
}
