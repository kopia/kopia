package cache

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
)

func TestNewStorageOrNil(t *testing.T) {
	ctx := testlogging.Context(t)

	// empty cache dir
	st, err := NewStorageOrNil(ctx, "", 1000, "subdir")
	require.NoError(t, err)
	require.Nil(t, st)

	// zero size
	st, err = NewStorageOrNil(ctx, testutil.TempDirectory(t), 0, "subdir")
	require.NoError(t, err)
	require.Nil(t, st)

	_, err = NewStorageOrNil(ctx, "relative/path/to/cache/dir", 1000, "subdir")
	require.Error(t, err)

	someError := errors.New("some error")

	oldMkdirAll := mkdirAll

	mkdirAll = func(path string, mode os.FileMode) error {
		return someError
	}

	defer func() {
		mkdirAll = oldMkdirAll
	}()

	_, err = NewStorageOrNil(ctx, filepath.Join(testutil.TempDirectory(t), "some-subdir"), 1000, "subdir")

	require.ErrorIs(t, err, someError)
}
