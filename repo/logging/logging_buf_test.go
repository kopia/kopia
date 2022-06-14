package logging_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/logging"
)

func TestLoggingBuffer_ReusesMemory(t *testing.T) {
	b := logging.GetBuffer()
	defer b.Release()

	b.AppendString("xx")

	s := b.String()
	require.Equal(t, "xx", s)

	// ensure we're reusing memory
	b.Reset()
	b.AppendString("yy")

	require.Equal(t, "yy", s)
}

func TestLoggingBuffer_Overflow(t *testing.T) {
	b := logging.GetBuffer()
	defer b.Release()

	filler := strings.Repeat("x", 1020)
	b.AppendString(filler)
	b.AppendString("foobarbaz")

	// only room for 4 more characters
	require.Equal(t, filler+"foob", b.String())

	b.Reset()

	b.AppendString(filler)
	b.AppendBytes([]byte{65, 66, 67, 68, 69})

	// only room for 4 more characters
	require.Equal(t, filler+"ABCD", b.String())
}

func TestLoggingBuffer_Append(t *testing.T) {
	b := logging.GetBuffer()
	defer b.Release()

	require.Equal(t, "", b.String())

	require.Equal(t,
		"xx ABC D -42 -23 true 42 false 23 2000-01-02T03:04:05Z",
		b.AppendString("xx").
			AppendString(" ").
			AppendBytes([]byte{65, 66, 67}).
			AppendString(" ").
			AppendByte('D').
			AppendString(" ").
			AppendInt32(-42).
			AppendString(" ").
			AppendInt64(-23).
			AppendString(" ").
			AppendBoolean(true).
			AppendString(" ").
			AppendUint32(42).
			AppendString(" ").
			AppendBoolean(false).
			AppendString(" ").
			AppendUint64(23).
			AppendString(" ").
			AppendTime(time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC), time.RFC3339).
			String())
}
