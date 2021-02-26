// Package restore manages restoring filesystem snapshots.
package restore

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/snapshot"
)

// shallowFilesystemOutput overrides methods in FilesystemOutput with
// shallow versions.
type shallowFilesystemOutput struct {
	*FilesystemOutput
}

func makeShallowFilesystemOutput(o Output) Output {
	fso, ok := o.(*FilesystemOutput)
	if ok {
		return &shallowFilesystemOutput{
			FilesystemOutput: fso,
		}
	}

	return o
}

// Parallelizable delegates to FilesystemOutput.
// BeginDirectory delegates to FilesystemOutput.
// FinishDirectory delegates to restore.Output interface.

// WriteDirEntry implements restore.Output interface.
func (o *shallowFilesystemOutput) WriteDirEntry(ctx context.Context, relativePath string, de *snapshot.DirEntry, e fs.Directory) error {
	placeholderpath, err := o.writeShallowEntry(ctx, relativePath, de)
	if err != nil {
		return errors.Wrap(err, "shallow WriteDirEntry")
	}

	return o.setAttributes(placeholderpath, e, readonlyfilemode)
}

// WriteFile implements restore.Output interface.
func (o *shallowFilesystemOutput) WriteFile(ctx context.Context, relativePath string, f fs.File) error {
	log(ctx).Debugf("(Shallow) WriteFile %v (%v bytes) %v, %v", filepath.Join(o.TargetPath, relativePath), f.Size(), f.Mode(), f.ModTime())

	mde, ok := f.(snapshot.HasDirEntry)
	if !ok {
		return errors.Errorf("fs object is not HasDirEntry?")
	}

	placeholderpath, err := o.writeShallowEntry(ctx, relativePath, mde.DirEntry())
	if err != nil {
		return errors.Wrap(err, "shallow WriteFile")
	}

	return o.setAttributes(placeholderpath, f, readonlyfilemode)
}

const readonlyfilemode = 0222

func (o *shallowFilesystemOutput) writeShallowEntry(ctx context.Context, relativePath string, de *snapshot.DirEntry) (string, error) {
	path := filepath.Join(o.TargetPath, filepath.FromSlash(relativePath))
	if _, err := os.Lstat(path); err == nil {
		// Having both a placeholder and a real will cause snapshot to fail. But
		// removing the real path risks destroying data forever.
		return "", errors.Errorf("real path %v exists. cowardly refusing to add placeholder", path)
	}

	log(ctx).Debugf("ShallowFilesystemOutput.writeShallowEntry %v ", path)

	// TODO(rjk): Conceivably one could write small files instead of writing files with metadata.
	placeholderpath, err := localfs.WriteShallowPlaceholder(path, de)
	if err != nil {
		return "", errors.Wrap(err, "error writing placeholder")
	}

	return placeholderpath, nil
}

// CreateSymlink identical to FilesystemOutput.

var _ Output = (*shallowFilesystemOutput)(nil)
