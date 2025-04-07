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

// ShallowFilesystemOutput overrides methods in FilesystemOutput with
// shallow versions.
type ShallowFilesystemOutput struct {
	FilesystemOutput

	// Files smaller than this will be written directly as part of the restore.
	MinSizeForPlaceholder int32
}

func makeShallowFilesystemOutput(o Output, options Options) Output {
	fso, ok := o.(*FilesystemOutput)
	if ok {
		return &ShallowFilesystemOutput{
			FilesystemOutput:      *fso,
			MinSizeForPlaceholder: options.MinSizeForPlaceholder,
		}
	}

	return o
}

// Parallelizable delegates to FilesystemOutput.
// BeginDirectory delegates to FilesystemOutput.
// FinishDirectory delegates to restore.Output interface.

// WriteDirEntry implements restore.Output interface.
func (o *ShallowFilesystemOutput) WriteDirEntry(ctx context.Context, relativePath string, de *snapshot.DirEntry, e fs.Directory) error {
	placeholderpath, err := o.writeShallowEntry(ctx, relativePath, de)
	if err != nil {
		return errors.Wrap(err, "shallow WriteDirEntry")
	}

	return o.setAttributes(placeholderpath, e, readonlyfilemode)
}

// WriteFile implements restore.Output interface.
func (o *ShallowFilesystemOutput) WriteFile(ctx context.Context, relativePath string, f fs.File, _ FileWriteProgress) error {
	log(ctx).Debugf("(Shallow) WriteFile %v (%v bytes) %v, %v", filepath.Join(o.TargetPath, relativePath), f.Size(), f.Mode(), f.ModTime())

	mde, ok := f.(snapshot.HasDirEntry)
	if !ok {
		return errors.Errorf("fs object '%s' is not HasDirEntry?", f.Name())
	}

	de := mde.DirEntry()

	// Write small files directly instead of writing placeholders.
	if de.FileSize < int64(o.MinSizeForPlaceholder) {
		return o.FilesystemOutput.WriteFile(ctx, relativePath, f, nil)
	}

	placeholderpath, err := o.writeShallowEntry(ctx, relativePath, de)
	if err != nil {
		return errors.Wrap(err, "shallow WriteFile")
	}

	return o.setAttributes(placeholderpath, f, readonlyfilemode)
}

const readonlyfilemode = 0o222

func (o *ShallowFilesystemOutput) writeShallowEntry(ctx context.Context, relativePath string, de *snapshot.DirEntry) (string, error) {
	path := filepath.Join(o.TargetPath, filepath.FromSlash(relativePath))
	if _, err := os.Lstat(path); err == nil {
		// Having both a placeholder and a real will cause snapshot to fail. But
		// removing the real path risks destroying data forever.
		return "", errors.Errorf("real path %v exists. cowardly refusing to add placeholder", path)
	}

	log(ctx).Debugf("ShallowFilesystemOutput.writeShallowEntry %v ", path)

	placeholderpath, err := localfs.WriteShallowPlaceholder(path, de)
	if err != nil {
		return "", errors.Wrap(err, "error writing placeholder")
	}

	return placeholderpath, nil
}

// CreateSymlink identical to FilesystemOutput.

var _ Output = (*ShallowFilesystemOutput)(nil)
