package pems

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func requireFileSize(t *testing.T, path string, sz int64) {
	fi, err := os.Stat(path)
	require.Nil(t, err)
	require.Equal(t, sz, fi.Size())
}

type nameAndSize struct {
	fnm string
	sz  int64
}

func TestKats_ExportPEMsAsFiles(t *testing.T) {
	tcs := []struct {
		path  string
		files []nameAndSize
		err   error
	}{
		{
			path: "test.pem",
			files: []nameAndSize{
				{
					fnm: "encrypted_private_key.bin",
					sz:  714,
				},
				{
					fnm: "certificate.bin",
					sz:  761,
				},
			},
		},
		{
			path: "test.short.pem",
			files: []nameAndSize{
				{
					fnm: "certificate.bin",
					sz:  597,
				},
			},
		},
		{
			path: "Spark_2k.pems.lst",
			err:  ErrNoPEMFound,
			files: []nameAndSize{
				{
					fnm: "valid_1.bin",
					sz:  761},
			},
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("[%d]: %q", i, tc.path), func(t *testing.T) {
			ctx := context.Background()
			bs, err := os.ReadFile(filepath.Join("testdata", tc.path))
			require.Nil(t, err)
			tdir := t.TempDir()
			err = ExportPEMsAsFiles(ctx, false, tdir, bs)
			if tc.err == nil {
				require.Nil(t, err)
			} else {
				require.ErrorIs(t, err, tc.err)
			}
			for _, nmsz := range tc.files {
				require.FileExists(t, filepath.Join(tdir, nmsz.fnm))
				requireFileSize(t, filepath.Join(tdir, nmsz.fnm), nmsz.sz)
			}
		})
	}
}
