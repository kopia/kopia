package mappedfs_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/fs/mappedfs"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
)

func TestFiles(t *testing.T) {
	t.Parallel()

	tmp := testutil.TempDirectory(t)
	defer os.RemoveAll(tmp)

	err := os.MkdirAll(filepath.Join(tmp, "a/b/c"), 0o700)
	assert.NoError(t, err)

	content := []byte("data")

	err = os.WriteFile(filepath.Join(tmp, "a/b/c/d.txt"), content, 0o700)
	assert.NoError(t, err)

	oe, err := localfs.NewEntry(tmp)
	assert.NoError(t, err)

	me, err := mappedfs.New(oe, &dummyMapper{
		mappings: map[string]string{
			filepath.Join(tmp, "a"): filepath.Join(tmp, "a/b/c"),
		},
	})
	assert.NoError(t, err)

	root, ok := me.(fs.Directory)
	assert.True(t, ok)

	ctx := testlogging.Context(t)

	ae, err := root.Child(ctx, "a")
	assert.NoError(t, err)
	assert.NotNil(t, ae)

	a, ok := ae.(fs.Directory)
	assert.True(t, ok)

	mf1e, err := a.Child(ctx, "d.txt")
	assert.NoError(t, err)
	assert.NotNil(t, mf1e)

	mf1, ok := mf1e.(fs.File)
	assert.True(t, ok)

	reader, err := mf1.Open(ctx)
	assert.NoError(t, err)

	result := make([]byte, len(content))
	_, err = reader.Read(result)
	assert.NoError(t, err)

	assert.Equal(t, content, result)
}

type dummyMapper struct {
	mappings map[string]string
}

func (d *dummyMapper) Apply(path string) (string, error) {
	result, ok := d.mappings[path]
	if !ok {
		return path, nil
	}

	return result, nil
}

func (d *dummyMapper) Close() {
}
