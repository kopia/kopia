package format

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
)

func TestFormatBlobCache(t *testing.T) {
	tempdir1 := testutil.TempDirectory(t)
	tempdir2 := filepath.Join(testutil.TempDirectory(t), "subdir")

	cases := []struct {
		desc      string
		fbc       blobCache
		isDurable bool
	}{
		{"NullCache", NewFormatBlobCache("", -1, clock.Now), false},
		{"DiskCache-Exists", NewFormatBlobCache(tempdir1, -1, clock.Now), true},
		{"DiskCache-NotExists", NewFormatBlobCache(tempdir2, -1, clock.Now), true},
		{"MemoryCache", NewFormatBlobCache("", 10*time.Second, clock.Now), true},
	}

	t.Run("Cases", func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.desc, func(t *testing.T) {
				t.Parallel()

				ctx := testlogging.Context(t)

				v, mtime, ok := tc.fbc.Get(ctx, "blob1")
				require.False(t, ok)
				require.Nil(t, v)
				require.Zero(t, mtime)

				mtime, err := tc.fbc.Put(ctx, "blob1", []byte{1, 2, 3})
				require.NoError(t, err)
				require.NotZero(t, mtime)

				_, err = tc.fbc.Put(ctx, "blob2", []byte{3, 4, 5})
				require.NoError(t, err)

				v2, mtime2, ok := tc.fbc.Get(ctx, "blob1")

				if tc.isDurable {
					require.True(t, ok)
					require.Equal(t, []byte{1, 2, 3}, v2)
					require.Equal(t, mtime, mtime2)
				} else {
					require.False(t, ok)
					require.Nil(t, v)
					require.Zero(t, mtime2)
				}

				time.Sleep(3 * time.Second)

				// overwrite
				mtime3, err := tc.fbc.Put(ctx, "blob1", []byte{2, 3, 4})
				require.NoError(t, err)
				require.Greater(t, mtime3, mtime2)

				v2, mtime4, ok := tc.fbc.Get(ctx, "blob1")

				if tc.isDurable {
					require.True(t, ok)
					require.Equal(t, []byte{2, 3, 4}, v2)
					require.Equal(t, mtime3, mtime4)
				} else {
					require.False(t, ok)
					require.Nil(t, v2)
					require.Zero(t, mtime4)
				}

				tc.fbc.Remove(ctx, []blob.ID{"blob1"})

				v3, mtime5, ok := tc.fbc.Get(ctx, "blob1")

				require.False(t, ok)
				require.Nil(t, v3)
				require.Zero(t, mtime5)
			})
		}
	})

	require.NoFileExists(t, filepath.Join(tempdir1, "blob1"))
	require.NoFileExists(t, filepath.Join(tempdir2, "blob1"))
	require.FileExists(t, filepath.Join(tempdir1, "blob2"))
	require.FileExists(t, filepath.Join(tempdir2, "blob2"))
}
