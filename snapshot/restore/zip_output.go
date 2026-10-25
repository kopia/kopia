package restore

import (
	"archive/zip"
	"os"
	"context"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
)

// ZipOutput contains the options for outputting a file system tree to a zip file.
type ZipOutput struct {
	w      io.Closer
	zf     *zip.Writer
	method uint16
}

// Parallelizable implements restore.Output interface.
func (o *ZipOutput) Parallelizable() bool {
	return false
}

// BeginDirectory implements restore.Output interface.
func (o *ZipOutput) BeginDirectory(_ context.Context, relativePath string, e fs.Directory) error {
	if relativePath == "" {
		return nil
	}

	// ZIP directory names must end in /, including empty directories which
	// cannot otherwise be inferred from regular-file paths in the archive.
	h := &zip.FileHeader{Name: relativePath + "/", Method: zip.Store}
	h.Modified = e.ModTime()
	h.SetMode(e.Mode() | os.ModeDir)

	if _, err := o.zf.CreateHeader(h); err != nil {
		return errors.Wrap(err, "error creating zip directory entry")
	}

	return nil
}

// FinishDirectory implements restore.Output interface.
//
//nolint:revive
func (o *ZipOutput) FinishDirectory(ctx context.Context, relativePath string, e fs.Directory) error {
	return nil
}

// WriteDirEntry implements restore.Output interface.
//
//nolint:revive
func (o *ZipOutput) WriteDirEntry(ctx context.Context, relativePath string, de *snapshot.DirEntry, e fs.Directory) error {
	return nil
}

// Close implements restore.Output interface.
func (o *ZipOutput) Close(_ context.Context) error {
	if err := o.zf.Close(); err != nil {
		return errors.Wrap(err, "error closing zip")
	}

	//nolint:wrapcheck
	return o.w.Close()
}

// WriteFile implements restore.Output interface.
func (o *ZipOutput) WriteFile(ctx context.Context, relativePath string, f fs.File, _ FileWriteProgress) error {
	r, err := f.Open(ctx)
	if err != nil {
		return errors.Wrap(err, "error opening file")
	}
	defer r.Close() //nolint:errcheck

	h := &zip.FileHeader{
		Name:   relativePath,
		Method: o.method,
	}

	h.Modified = f.ModTime()
	h.SetMode(f.Mode())

	w, err := o.zf.CreateHeader(h)
	if err != nil {
		return errors.Wrap(err, "error creating zip entry")
	}

	if _, err := io.Copy(w, r); err != nil {
		return errors.Wrap(err, "error copying data to zip")
	}

	return nil
}

// FileExists implements restore.Output interface.
//
//nolint:revive
func (o *ZipOutput) FileExists(ctx context.Context, relativePath string, l fs.File) bool {
	return false
}

// CreateSymlink implements restore.Output interface.
func (o *ZipOutput) CreateSymlink(ctx context.Context, relativePath string, e fs.Symlink) error {
	target, err := e.Readlink(ctx)
	if err != nil {
		return errors.Wrap(err, "error reading zip symlink target")
	}

	// In ZIP's Unix convention symlinks carry ModeSymlink and the link target
	// as file contents. Never resolve or follow the link while exporting it.
	h := &zip.FileHeader{Name: relativePath, Method: zip.Store}
	h.Modified = e.ModTime()
	h.SetMode(e.Mode() | os.ModeSymlink)

	w, err := o.zf.CreateHeader(h)
	if err != nil {
		return errors.Wrap(err, "error creating zip symlink entry")
	}

	n, err := io.WriteString(w, target)
	if err != nil {
		return errors.Wrap(err, "error writing zip symlink target")
	}
	if n != len(target) {
		return io.ErrShortWrite
	}

	return nil
}

// SymlinkExists implements restore.Output interface.
//
//nolint:revive
func (o *ZipOutput) SymlinkExists(ctx context.Context, relativePath string, l fs.Symlink) bool {
	return false
}

// NewZipOutput creates new zip writer output.
func NewZipOutput(w io.WriteCloser, method uint16) *ZipOutput {
	return &ZipOutput{w, zip.NewWriter(w), method}
}

var _ Output = (*ZipOutput)(nil)
