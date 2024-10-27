package gather

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"testing"
	"testing/iotest"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

var sample1 = []byte("hello! how are you? nice to meet you.")

type failingWriter struct {
	err error
}

func (w failingWriter) Write(buf []byte) (int, error) {
	return 0, w.err
}

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
		func() {
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

			n, err := b.WriteTo(&tmp)

			require.NoError(t, err)
			require.Equal(t, int64(b.Length()), n)

			require.Equal(t, tmp.ToByteSlice(), b.ToByteSlice())

			someError := errors.New("some error")

			// WriteTo propagates error
			if b.Length() > 0 {
				_, err = b.WriteTo(failingWriter{someError})

				require.ErrorIs(t, err, someError)
			}

			require.Error(t, b.AppendSectionTo(&tmp, -3, 3))

			for i := 0; i <= len(tc.whole); i++ {
				for j := i; j <= len(tc.whole); j++ {
					tmp.Reset()

					require.NoError(t, b.AppendSectionTo(&tmp, i, j-i))

					if j > i {
						require.ErrorIs(t, b.AppendSectionTo(failingWriter{someError}, i, j-i), someError)
					}

					require.Equal(t, tmp.ToByteSlice(), tc.whole[i:j])
				}
			}
		}()
	}
}

func TestGatherBytesReadSeeker(t *testing.T) {
	var tmp WriteBuffer
	defer tmp.Close()

	buf := make([]byte, 1234567)

	tmp.Append(buf)

	require.Len(t, buf, tmp.Length())

	reader := tmp.inner.Reader()
	defer reader.Close() //nolint:errcheck

	// TestReader tests that reading from r returns the expected file content.
	// It does reads of different sizes, until EOF.
	// If r implements [io.ReaderAt] or [io.Seeker], TestReader also checks
	// that those operations behave as they should.
	//
	// If TestReader finds any misbehaviors, it returns an error reporting them.
	// The error text may span multiple lines.
	require.NoError(t, iotest.TestReader(reader, buf))

	_, err := reader.Seek(-3, io.SeekStart)
	require.Error(t, err)

	_, err = reader.Seek(3, io.SeekEnd)
	require.Error(t, err)

	_, err = reader.Seek(10000000, io.SeekCurrent)
	require.Error(t, err)
}

func TestGatherBytesReaderAtErrorResponses(t *testing.T) {
	// 3.7 times the internal chunk size
	contentBuf := make([]byte, int(float64(defaultAllocator.chunkSize)*3.7))
	for i := range contentBuf {
		contentBuf[i] = uint8(i % math.MaxUint8)
	}

	tcs := []struct {
		inBsLen   int
		inOff     int64
		expectErr error
		expectN   int
	}{
		{
			inBsLen:   1 << 10,
			inOff:     -1,
			expectErr: ErrInvalidOffset,
			expectN:   0,
		},
		{
			inBsLen:   1 << 10,
			inOff:     math.MaxInt64,
			expectErr: io.EOF,
			expectN:   0,
		},
		{
			inBsLen:   0,
			inOff:     -1,
			expectErr: ErrInvalidOffset,
			expectN:   0,
		},
		{
			inBsLen: 0,
			inOff:   math.MaxInt64,
			expectN: 0,
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d: %d %d %d", i, tc.inBsLen, tc.inOff, tc.expectN), func(t *testing.T) {
			// tmp is an empty buffer that will supply some bytes
			// for testing
			var wrt WriteBuffer
			defer wrt.Close()

			wrt.Append(contentBuf)
			require.Equalf(t, defaultAllocator.chunkSize, wrt.alloc.chunkSize,
				"this test expects that the default-allocator will be used, but we are using: %#v", wrt.alloc)

			// get the reader out of the WriteBuffer so we can read what was written
			// (presume all 0s)
			reader := wrt.inner.Reader()
			defer reader.Close() //nolint:errcheck

			// get the reader as a ReaderAt
			readerAt := reader.(io.ReaderAt)

			// make an output buffer of the required length
			bs := make([]byte, tc.inBsLen)

			n, err := readerAt.ReadAt(bs, tc.inOff)
			require.ErrorIs(t, err, tc.expectErr)
			require.Equal(t, tc.expectN, n)
		})
	}
}

func TestGatherBytesReaderAtVariableInputBufferSizes(t *testing.T) {
	const inputBufferMaxMultiplier = 4.0 // maximum number of times the internal chunk size

	contentBuf := make([]byte, defaultAllocator.chunkSize*inputBufferMaxMultiplier)
	for i := range contentBuf {
		contentBuf[i] = uint8(i % math.MaxUint8)
	}

	type testCase struct {
		name            string
		inputBufferSize int
	}

	// Test some interesting input buffer sizes from a 1-byte buffer to many multiples
	// of the internal allocator chunk size.
	testCases := []testCase{
		{"1", 1},
		{"0.5x", int(0.5 * float64(defaultAllocator.chunkSize))},

		{"x-1", defaultAllocator.chunkSize - 1},
		{"x", defaultAllocator.chunkSize},
		{"x+1", defaultAllocator.chunkSize + 1},
		{"1.5x", int(1.5 * float64(defaultAllocator.chunkSize))},

		{"2x-1", 2*defaultAllocator.chunkSize - 1},
		{"2x", 2 * defaultAllocator.chunkSize},
		{"2x+1", 2*defaultAllocator.chunkSize + 1},
		{"2.5x", int(2.5 * float64(defaultAllocator.chunkSize))},

		{"3x-1", 3*defaultAllocator.chunkSize - 1},
		{"3x", 3 * defaultAllocator.chunkSize},
		{"3x+1", 3*defaultAllocator.chunkSize + 1},

		{"4x-1", 4*defaultAllocator.chunkSize - 1},
		{"4x", 4 * defaultAllocator.chunkSize},
	}

	// Test the third buffer slice. The idea here is to exercise the part of
	// the buffer ReaderAt implementation where it has a longer buffer size
	// than the size of the internal chunks of the buffer implementation. When
	// we do this, the ReaderAt is forced to draw more data than it actually
	// can from the first slice it found after searching for the current
	// pointer in read cycle. Finally, it should increment the read index
	// correctly.
	//
	// x.1 ... x.9
	for chunkSizeMultiplier := inputBufferMaxMultiplier - 0.9; chunkSizeMultiplier < inputBufferMaxMultiplier; chunkSizeMultiplier += 0.1 {
		testCases = append(testCases, testCase{
			fmt.Sprintf("%.1fx", chunkSizeMultiplier),
			int(float64(defaultAllocator.chunkSize) * chunkSizeMultiplier),
		},
		)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// each test should have its own writer because t.Run() can be
			// parallelized
			var preWrt WriteBuffer
			defer preWrt.Close()

			// assert some preconditions that the reader conforms to ReaderAt
			buf := contentBuf[:tc.inputBufferSize]

			// write the generated data
			n, err := preWrt.Write(buf)
			require.NoErrorf(t, err, "Write() faiiled, inputBufferSize: %v", tc.inputBufferSize)
			require.Equalf(t, defaultAllocator.chunkSize, preWrt.alloc.chunkSize,
				"this test expects that the default-allocator will be used, but we are using: %#v", preWrt.alloc)

			require.Lenf(t, buf, n, "unexpected size of data written, inputBufferSize: %d", tc.inputBufferSize)

			// get the reader out of the WriteBuffer so we can read what was written
			preRdr := preWrt.inner.Reader()
			_, ok := preRdr.(io.ReaderAt)
			require.Truef(t, ok, "MUST implement io.ReaderAt, inputBufferSize: %d", tc.inputBufferSize)

			// execute standard ReadAt tests.
			require.NoErrorf(t, iotest.TestReader(preRdr, buf),
				"iotest failed, inputBufferSize: %d", tc.inputBufferSize)
		})
	}
}

func TestGatherBytesPanicsOnClose(t *testing.T) {
	var tmp WriteBuffer

	tmp.Append([]byte{1, 2, 3})
	tmp.Close()

	require.Panics(t, func() {
		tmp.Bytes().Reader()
	})
}
