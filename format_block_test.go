package repo

import (
	"context"
	"crypto/sha256"
	"reflect"
	"testing"

	"github.com/kopia/repo/internal/storagetesting"
	"github.com/kopia/repo/storage"
)

func TestFormatBlockRecovery(t *testing.T) {
	data := map[string][]byte{}
	st := storagetesting.NewMapStorage(data, nil, nil)
	ctx := context.Background()

	someDataBlock := []byte("aadsdasdas")
	checksummed, err := addFormatBlockChecksumAndLength(someDataBlock)
	if err != nil {
		t.Errorf("error appending checksum: %v", err)
	}
	if got, want := len(checksummed), 2+2+sha256.Size+len(someDataBlock); got != want {
		t.Errorf("unexpected checksummed length: %v, want %v", got, want)
	}

	st.PutBlock(ctx, "some-block-by-itself", checksummed)
	st.PutBlock(ctx, "some-block-suffix", append(append([]byte(nil), 1, 2, 3), checksummed...))
	st.PutBlock(ctx, "some-block-prefix", append(append([]byte(nil), checksummed...), 1, 2, 3))

	// mess up checksum
	checksummed[len(checksummed)-3] ^= 1
	st.PutBlock(ctx, "bad-checksum", checksummed)
	st.PutBlock(ctx, "zero-len", []byte{})
	st.PutBlock(ctx, "one-len", []byte{1})
	st.PutBlock(ctx, "two-len", []byte{1, 2})
	st.PutBlock(ctx, "three-len", []byte{1, 2, 3})
	st.PutBlock(ctx, "four-len", []byte{1, 2, 3, 4})
	st.PutBlock(ctx, "five-len", []byte{1, 2, 3, 4, 5})

	cases := []struct {
		block string
		err   error
	}{
		{"some-block-by-itself", nil},
		{"some-block-suffix", nil},
		{"some-block-prefix", nil},
		{"bad-checksum", errFormatBlockNotFound},
		{"no-such-block", storage.ErrBlockNotFound},
		{"zero-len", errFormatBlockNotFound},
		{"one-len", errFormatBlockNotFound},
		{"two-len", errFormatBlockNotFound},
		{"three-len", errFormatBlockNotFound},
		{"four-len", errFormatBlockNotFound},
		{"five-len", errFormatBlockNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.block, func(t *testing.T) {
			v, err := RecoverFormatBlock(ctx, st, tc.block, -1)
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
