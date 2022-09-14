package tempfile_test

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/tempfile"
)

func TestTempFile(t *testing.T) {
	td := t.TempDir()

	f, err := tempfile.Create(td)
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

	files, err := os.ReadDir(td)
	require.NoError(t, err)
	require.Empty(t, files)
}
