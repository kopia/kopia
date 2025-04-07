package dirutil_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/dirutil"
	"github.com/kopia/kopia/internal/testutil"
)

type testOSI struct {
	t        *testing.T
	mkdirErr error
}

func (testOSI) IsPathSeparator(c byte) bool { return os.IsPathSeparator(c) }
func (testOSI) IsExist(err error) bool      { return os.IsExist(err) }
func (testOSI) IsNotExist(err error) bool   { return os.IsNotExist(err) }
func (r testOSI) Mkdir(name string, perm os.FileMode) error {
	if r.mkdirErr != nil {
		r.t.Logf("returning error for %v", name)
		return r.mkdirErr
	}

	r.t.Logf("creating %v", name)

	return os.Mkdir(name, perm)
}

func TestMkSubdirAll(t *testing.T) {
	osi := testOSI{t, nil}

	td := testutil.TempDirectory(t)

	// notice - cases are evaluated in order
	cases := []struct {
		topLevelDir string
		subDir      string
		wantErr     error
	}{
		{td, td, dirutil.ErrTopLevelDirectoryNotFound},
		{filepath.Join(td, "subdir2"), td, dirutil.ErrTopLevelDirectoryNotFound},
		{td, filepath.Join(td, "subdir1"), nil},            // will create one subdirectory
		{td, filepath.Join(td, "subdir2", "subdir3"), nil}, // will create two subdirectories
		{td, filepath.Join(td, "subdir2"), nil},            // already exists
	}

	for _, tc := range cases {
		err := dirutil.MkSubdirAll(osi, tc.topLevelDir, tc.subDir, 0o755)
		if tc.wantErr == nil {
			require.NoError(t, err)
			require.DirExists(t, tc.subDir)
		} else {
			require.ErrorIs(t, err, tc.wantErr)
		}
	}

	osi.mkdirErr = errors.New("some error")

	require.ErrorIs(t, dirutil.MkSubdirAll(osi, td, filepath.Join(td, "somedir4"), 0o755), osi.mkdirErr)
}
