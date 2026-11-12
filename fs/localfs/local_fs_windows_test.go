package localfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/testutil"
)

// Hardlink info is not yet available on Windows; verify the zero value is returned.
func TestHardLinkInfoUnsupported(t *testing.T) {
	tmp := testutil.TempDirectory(t)

	target := filepath.Join(tmp, "target")
	link := filepath.Join(tmp, "link")

	require.NoError(t, os.WriteFile(target, []byte{1, 2, 3}, 0o644))
	require.NoError(t, os.Link(target, link))

	e, err := NewEntry(link)
	require.NoError(t, err)
	require.Equal(t, fs.HardLinkInfo{}, e.HardLinkInfo())
}
