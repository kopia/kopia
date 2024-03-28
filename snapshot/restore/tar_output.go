package restore

import (
	"archive/tar"
	"context"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
)

// TarOutput contains the options for outputting a file system tree to a tar or .tar.gz file.
type TarOutput struct {
	w  io.Closer
	tf *tar.Writer
}

// Parallelizable implements restore.Output interface.
func (o *TarOutput) Parallelizable() bool {
	return false
}

// BeginDirectory implements restore.Output interface.
func (o *TarOutput) BeginDirectory(ctx context.Context, relativePath string, d fs.Directory) error {
	if relativePath == "" {
		return nil
	}

	h := &tar.Header{
		Name:     relativePath + "/",
		ModTime:  d.ModTime(),
		Mode:     int64(d.Mode()),
		Uid:      int(d.Owner().UserID),
		Gid:      int(d.Owner().GroupID),
		Typeflag: tar.TypeDir,
	}

	if err := o.tf.WriteHeader(h); err != nil {
		return errors.Wrap(err, "error writing tar header")
	}

	return nil
}

// FinishDirectory implements restore.Output interface.
//
//nolint:revive
func (o *TarOutput) FinishDirectory(ctx context.Context, relativePath string, e fs.Directory) error {
	return nil
}

// WriteDirEntry implements restore.Output interface.
//
//nolint:revive
func (o *TarOutput) WriteDirEntry(ctx context.Context, relativePath string, de *snapshot.DirEntry, e fs.Directory) error {
	return nil
}

// Close implements restore.Output interface.
func (o *TarOutput) Close(ctx context.Context) error {
	if err := o.tf.Close(); err != nil {
		return errors.Wrap(err, "error closing tar")
	}

	//nolint:wrapcheck
	return o.w.Close()
}

// WriteFile implements restore.Output interface.
func (o *TarOutput) WriteFile(ctx context.Context, relativePath string, f fs.File, _ FileWriteProgress) error {
	r, err := f.Open(ctx)
	if err != nil {
		return errors.Wrap(err, "error opening file")
	}
	defer r.Close() //nolint:errcheck

	h := &tar.Header{
		Name:     relativePath,
		ModTime:  f.ModTime(),
		Size:     f.Size(),
		Mode:     int64(f.Mode()),
		Uid:      int(f.Owner().UserID),
		Gid:      int(f.Owner().GroupID),
		Typeflag: tar.TypeReg,
	}

	if err := o.tf.WriteHeader(h); err != nil {
		return errors.Wrap(err, "error writing tar header")
	}

	if _, err := io.Copy(o.tf, r); err != nil {
		return errors.Wrap(err, "error copying data to tar")
	}

	return nil
}

// FileExists implements restore.Output interface.
//
//nolint:revive
func (o *TarOutput) FileExists(ctx context.Context, relativePath string, f fs.File) bool {
	return false
}

// CreateSymlink implements restore.Output interface.
func (o *TarOutput) CreateSymlink(ctx context.Context, relativePath string, l fs.Symlink) error {
	target, err := l.Readlink(ctx)
	if err != nil {
		return errors.Wrap(err, "error reading link target")
	}

	h := &tar.Header{
		Name:     relativePath,
		ModTime:  l.ModTime(),
		Mode:     int64(l.Mode()),
		Uid:      int(l.Owner().UserID),
		Gid:      int(l.Owner().GroupID),
		Typeflag: tar.TypeSymlink,
		Linkname: target,
	}

	if err := o.tf.WriteHeader(h); err != nil {
		return errors.Wrap(err, "error writing tar header")
	}

	return nil
}

// SymlinkExists implements restore.Output interface.
//
//nolint:revive
func (o *TarOutput) SymlinkExists(ctx context.Context, relativePath string, l fs.Symlink) bool {
	return false
}

// NewTarOutput creates new tar writer output.
func NewTarOutput(w io.WriteCloser) *TarOutput {
	return &TarOutput{w, tar.NewWriter(w)}
}

var _ Output = (*TarOutput)(nil)
