package tempfile

import (
	"io"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func VerifyTempfile(t *testing.T, create func() (*os.File, error)) {
	t.Helper()

	f, err := create()
	require.NoError(t, err)

	n, err := f.WriteString("hello")
	require.NoError(t, err)
	require.Equal(t, 5, n)

	off, err := f.Seek(1, io.SeekStart)
	require.Equal(t, int64(1), off)
	require.NoError(t, err)

	buf := make([]byte, 4)
	n2, err := f.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 4, n2)
	require.Equal(t, []byte("ello"), buf)

	f.Close()

	if n := f.Name(); n != "" {
		var perr *os.PathError

		// there should be no directory entry for this file
		_, err := os.Stat(n)

		require.Error(t, err)
		require.ErrorAs(t, err, &perr)
		if runtime.GOOS == "windows" {
			require.ErrorContains(t, err, "The system cannot find the file specified")
		} else {
			require.ErrorContains(t, err, "no such file or directory")
		}
	}
}
