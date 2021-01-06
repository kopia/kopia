// Package restore manages restoring filesystem snapshots.
package restore

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
)

// ShallowFilesystemOutput contains the options for doing a shallow output of a filesystem tree.
type ShallowFilesystemOutput struct {
	FilesystemOutput
}

// Parallelizable delegates to FilesystemOutput.
// BeginDirectory delegates to FilesystemOutput.
// FinishDirectory delegates to restore.Output interface.

// WriteShallowDirectory implements restore.Output interface.
func (o *ShallowFilesystemOutput) WriteShallowDirectory(ctx context.Context, relativePath string, e fs.Directory) error {
	return o.writeShallowEntry(ctx, relativePath, e)
}

// WriteFile implements restore.Output interface.
func (o *ShallowFilesystemOutput) WriteFile(ctx context.Context, relativePath string, f fs.File) error {
	log(ctx).Debugf("(Shallow) WriteFile %v (%v bytes) %v, %v", filepath.Join(o.TargetPath, relativePath), f.Size(), f.Mode(), f.ModTime())
	return o.writeShallowEntry(ctx, relativePath, f)
}

const readonlyfilemode = 0222

func (o *ShallowFilesystemOutput) writeShallowEntry(ctx context.Context, relativePath string, f fs.Entry) error {
	path := filepath.Join(o.TargetPath, filepath.FromSlash(relativePath))
	if _, err := os.Lstat(path); err == nil {
		// Having both a placeholder and a real will cause snapshot to fail. But
		// removing the real path risks destroying data forever.
		return errors.Errorf("real path %v exists. cowardly refusing to add placeholder", path)
	}

	log(ctx).Debugf("ShallowFilesystemOutput.writeShallowEntry %v ", path)

	// TODO(rjk): Conceivably one could write small files instead of writing files with metadata.
	placeholderpath, err := localfs.WriteShallowPlaceholder(path, f)
	if err != nil {
		return errors.Wrap(err, "error writing placeholder")
	}

	return o.setAttributes(placeholderpath, f, readonlyfilemode)
}

// CreateSymlink identical to FilesystemOutput.

var _ Output = (*ShallowFilesystemOutput)(nil)
