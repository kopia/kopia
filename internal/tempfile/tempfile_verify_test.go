package tempfile

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func VerifyTempfile(t *testing.T, create func(dir string) (*os.File, error)) {
	t.Helper()

	cases := []struct {
		name string
		dir  string
	}{
		{
			name: "empty dir name",
			dir:  "",
		},
		{
			name: "non-empty dir name",
			dir:  t.TempDir(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testDir := tc.dir

			f, err := create(testDir)
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

			if testDir != "" { // $TEMPDIR often has other files, so it does not make sense to check whether it is empty
				files, err := os.ReadDir(testDir)
				require.NoError(t, err)
				require.Emptyf(t, files, "number of files: %v", len(files))
			}
		})
	}
}
