package gather

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

var sample1 = []byte("hello! how are you? nice to meet you.")

func TestGatherBytes(t *testing.T) {
	// split the 'whole' into equivalent Bytes slicings in some interesting ways
	cases := []struct {
		whole  []byte
		sliced Bytes
	}{
		{
			whole:  []byte{},
			sliced: Bytes{},
		},
		{
			whole: []byte{},
			sliced: Bytes{Slices: [][]byte{
				nil,
			}},
		},
		{
			whole: []byte{},
			sliced: Bytes{Slices: [][]byte{
				nil,
				{},
				nil,
			}},
		},
		{
			whole:  sample1,
			sliced: FromSlice(sample1),
		},
		{
			whole: sample1,
			sliced: Bytes{Slices: [][]byte{
				nil,
				sample1,
				nil,
			}},
		},
		{
			whole: sample1,
			sliced: Bytes{Slices: [][]byte{
				sample1[0:20],
				sample1[20:],
			}},
		},
		{
			whole: sample1,
			sliced: Bytes{Slices: [][]byte{
				sample1[0:20],
				nil, // zero-length
				{},  // zero-length
				sample1[20:],
			}},
		},
		{
			whole: sample1,
			sliced: Bytes{Slices: [][]byte{
				sample1[0:10],
				sample1[10:25],
				sample1[25:30],
				sample1[30:31],
				sample1[31:],
			}},
		},
	}

	for _, tc := range cases {
		b := tc.sliced

		// length
		if got, want := b.Length(), len(tc.whole); got != want {
			t.Errorf("unexpected length: %v, want %v", got, want)
		}

		// reader
		all, err := io.ReadAll(b.Reader())
		if err != nil {
			t.Errorf("unable to read: %v", err)
		}

		if !bytes.Equal(all, tc.whole) {
			t.Errorf("unexpected data read %v, want %v", string(all), string(tc.whole))
		}

		// GetBytes
		all = b.ToByteSlice()
		if !bytes.Equal(all, tc.whole) {
			t.Errorf("unexpected data from GetBytes() %v, want %v", string(all), string(tc.whole))
		}

		// AppendSectionTo - test exhaustively all combinationf os start, length
		var tmp WriteBuffer
		defer tmp.Close()

		for i := 0; i <= len(tc.whole); i++ {
			for j := i; j <= len(tc.whole); j++ {
				tmp.Reset()

				b.AppendSectionTo(&tmp, i, j-i)

				require.Equal(t, tmp.ToByteSlice(), tc.whole[i:j])
			}
		}
	}
}
