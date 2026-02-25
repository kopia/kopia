package restore

import (
	"context"
	stderrors "errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/atomicfile"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/sparsefile"
	"github.com/kopia/kopia/internal/stat"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

const (
	outputDirMode                     = 0o700 // default mode to create directories in before setting their ACLs
	maxTimeDeltaToConsiderFileTheSame = 2 * time.Second
)

// streamCopier is a generic function type to perform the actual copying of data bits
// from a source stream to a destination stream.
type streamCopier func(io.WriteSeeker, io.Reader) (int64, error)

// getStreamCopier returns a function that can copy data from a source stream to a destination stream.
func getStreamCopier(ctx context.Context, targetpath string, sparse bool) (streamCopier, error) {
	if sparse {
		if !isWindows() {
			dirpath := filepath.Dir(targetpath)

			s, err := stat.GetBlockSize(dirpath)
			if err != nil {
				return nil, errors.Wrapf(err, "error getting disk block size for target %v", dirpath)
			}

			return func(w io.WriteSeeker, r io.Reader) (int64, error) {
				return sparsefile.Copy(w, r, s)
			}, nil
		}

		log(ctx).Debug("sparse copying is not supported on Windows, falling back to regular copying")
	}

	// Wrap iocopy.Copy to conform to StreamCopier type.
	return func(w io.WriteSeeker, r io.Reader) (int64, error) {
		return iocopy.Copy(w, r)
	}, nil
}

// progressReportingReader wraps fs.Reader Read function to capture the and pass
// the number of bytes read to the callback cb.
type progressReportingReader struct {
	fs.Reader

	cb FileWriteProgress
}

func (r *progressReportingReader) Read(p []byte) (int, error) {
	bytesRead, err := r.Reader.Read(p)
	if err == nil && r.cb != nil {
		r.cb(int64(bytesRead))
	}

	return bytesRead, err //nolint:wrapcheck
}

// parallelChunkedFile is optionally implemented by fs.File to support parallel chunk restore.
// It is satisfied by snapshotfs.repositoryFile for multi-chunk indirect objects.
type parallelChunkedFile interface {
	ReadChunksParallel(ctx context.Context, workers int, callback func(offset int64, data []byte) error) error
}

// FilesystemOutput contains the options for outputting a file system tree.
type FilesystemOutput struct {
	// TargetPath for restore.
	TargetPath string `json:"targetPath"`

	// If a directory already exists, overwrite the directory.
	OverwriteDirectories bool `json:"overwriteDirectories"`

	// Indicate whether or not to overwrite existing files. When set to false,
	// the copier does not modify already existing files and returns an error
	// instead.
	OverwriteFiles bool `json:"overwriteFiles"`

	// If a symlink already exists, remove it and create a new one. When set to
	// false, the copier does not modify existing symlinks and will return an
	// error instead.
	OverwriteSymlinks bool `json:"overwriteSymlinks"`

	// IgnorePermissionErrors causes restore to ignore errors due to invalid permissions.
	IgnorePermissionErrors bool `json:"ignorePermissionErrors"`

	// When set to true, first write to a temp file and rename it, to ensure there are no partially written files in case of a crash.
	WriteFilesAtomically bool `json:"writeFilesAtomically"`

	// SkipOwners when set to true causes restore to skip restoring owner information.
	SkipOwners bool `json:"skipOwners"`

	// SkipPermissions when set to true causes restore to skip restoring permission information.
	SkipPermissions bool `json:"skipPermissions"`

	// SkipTimes when set to true causes restore to skip restoring modification times.
	SkipTimes bool `json:"skipTimes"`

	// WriteSparseFiles when set to true, write contents as sparse files, minimizing allocated disk space.
	WriteSparseFiles bool `json:"writeSparseFiles"`

	// copier is the StreamCopier to use for copying the actual bit stream to output.
	// It is assigned at runtime based on the target filesystem and restore options.
	copier streamCopier `json:"-"`

	// Indicate whether or not flush files after restore.
	// Varying from OS, the copier may write the file data to the system cache,
	// so the data may not be written to disk when the restore to the file completes.
	// This flag guarantees the file data is flushed to disk.
	FlushFiles bool `json:"flushFiles"`

	// ParallelChunkWorkers sets the number of concurrent goroutines used to fetch
	// chunks of a single large file during restore. When > 1, chunks are fetched in
	// parallel and written via WriteAt, which significantly improves restore speed for
	// large files on high-latency storage (e.g. S3). 0 means use the default (8).
	// Has no effect when WriteFilesAtomically or WriteSparseFiles is set.
	ParallelChunkWorkers int `json:"parallelChunkWorkers"`
}

// Init initializes the internal members of the filesystem writer output.
// This method must be called before FilesystemOutput can be used.
func (o *FilesystemOutput) Init(ctx context.Context) error {
	c, err := getStreamCopier(ctx, o.TargetPath, o.WriteSparseFiles)
	if err != nil {
		return errors.Wrap(err, "unable to get stream copier")
	}

	o.copier = c

	return nil
}

// Parallelizable implements restore.Output interface.
func (o *FilesystemOutput) Parallelizable() bool {
	return true
}

// BeginDirectory implements restore.Output interface.
func (o *FilesystemOutput) BeginDirectory(ctx context.Context, relativePath string, _ fs.Directory) error {
	path := filepath.Join(o.TargetPath, filepath.FromSlash(relativePath))

	if err := o.createDirectory(ctx, path); err != nil {
		return errors.Wrap(err, "error creating directory")
	}

	return nil
}

// FinishDirectory implements restore.Output interface.
func (o *FilesystemOutput) FinishDirectory(_ context.Context, relativePath string, e fs.Directory) error {
	path := filepath.Join(o.TargetPath, filepath.FromSlash(relativePath))
	if err := o.setAttributes(path, e, os.FileMode(0)); err != nil {
		return errors.Wrap(err, "error setting attributes")
	}

	return SafeRemoveAll(path)
}

// WriteDirEntry implements restore.Output interface.
//
//nolint:revive
func (o *FilesystemOutput) WriteDirEntry(ctx context.Context, relativePath string, de *snapshot.DirEntry, e fs.Directory) error {
	return nil
}

// Close implements restore.Output interface.
func (o *FilesystemOutput) Close(_ context.Context) error {
	return nil
}

// WriteFile implements restore.Output interface.
func (o *FilesystemOutput) WriteFile(ctx context.Context, relativePath string, f fs.File, progressCb FileWriteProgress) error {
	log(ctx).Debugf("WriteFile %v (%v bytes) %v, %v", filepath.Join(o.TargetPath, relativePath), f.Size(), f.Mode(), f.ModTime())
	path := filepath.Join(o.TargetPath, filepath.FromSlash(relativePath))

	if err := o.copyFileContent(ctx, path, f, progressCb); err != nil {
		return errors.Wrap(err, "error creating file")
	}

	if err := o.setAttributes(path, f, os.FileMode(0)); err != nil {
		return errors.Wrap(err, "error setting attributes")
	}

	return SafeRemoveAll(path)
}

// FileExists implements restore.Output interface.
func (o *FilesystemOutput) FileExists(_ context.Context, relativePath string, e fs.File) bool {
	st, err := os.Lstat(filepath.Join(o.TargetPath, relativePath))
	if err != nil {
		return false
	}

	if (st.Mode() & os.ModeType) != 0 {
		// not a file
		return false
	}

	if st.Size() != e.Size() {
		// wrong size
		return false
	}

	timeDelta := st.ModTime().Sub(e.ModTime())
	if timeDelta < 0 {
		timeDelta = -timeDelta
	}

	return timeDelta < maxTimeDeltaToConsiderFileTheSame
}

// CreateSymlink implements restore.Output interface.
func (o *FilesystemOutput) CreateSymlink(ctx context.Context, relativePath string, e fs.Symlink) error {
	targetPath, err := e.Readlink(ctx)
	if err != nil {
		return errors.Wrap(err, "error reading link target")
	}

	log(ctx).Debugf("CreateSymlink %v => %v, time %v", filepath.Join(o.TargetPath, relativePath), targetPath, e.ModTime())

	path := filepath.Join(o.TargetPath, filepath.FromSlash(relativePath))

	switch st, err := os.Lstat(path); {
	case os.IsNotExist(err): // Proceed to symlink creation
	case err != nil:
		return errors.Wrap(err, "lstat error at symlink path")
	case fileIsSymlink(st):
		// Throw error if we are not overwriting symlinks
		if !o.OverwriteSymlinks {
			return errors.New("will not overwrite existing symlink")
		}

		// Remove the existing symlink before symlink creation
		if err := os.Remove(path); err != nil {
			return errors.Wrap(err, "removing existing symlink")
		}
	default:
		return errors.Errorf("unable to create symlink, %q already exists and is not a symlink", path)
	}

	if err := os.Symlink(targetPath, path); err != nil {
		return errors.Wrap(err, "error creating symlink")
	}

	if err := o.setAttributes(path, e, os.FileMode(0)); err != nil {
		return errors.Wrap(err, "error setting attributes")
	}

	return nil
}

func fileIsSymlink(st os.FileInfo) bool {
	return st.Mode()&os.ModeSymlink != 0
}

// SymlinkExists implements restore.Output interface.
//
//nolint:revive
func (o *FilesystemOutput) SymlinkExists(ctx context.Context, relativePath string, e fs.Symlink) bool {
	st, err := os.Lstat(filepath.Join(o.TargetPath, relativePath))
	if err != nil {
		return false
	}

	return (st.Mode() & os.ModeType) == os.ModeSymlink
}

// setAttributes sets permission, modification time and user/group ids
// on targetPath. modclear will clear the specified FileMod bits. Pass 0
// to not clear any.
func (o *FilesystemOutput) setAttributes(targetPath string, e fs.Entry, modclear os.FileMode) error {
	le, err := localfs.NewEntry(targetPath)
	if err != nil {
		return errors.Wrap(err, "could not create local FS entry for "+targetPath)
	}

	var (
		osChmod   = os.Chmod
		osChown   = os.Chown
		osChtimes = os.Chtimes
	)

	// symbolic links require special handling that is OS-specific and sometimes unsupported
	// os.* functions change the target of the symlink and not the symlink itself.
	if isSymlink(e) {
		osChmod, osChown, osChtimes = symlinkChmod, symlinkChown, symlinkChtimes
		modclear = os.FileMode(0)
	}

	// Set owner user and group from e
	// On Windows Chown is not supported. fs.OwnerInfo collected on Windows will always
	// be zero-value for UID and GID, so the Chown operation is not performed.
	if o.shouldUpdateOwner(le, e) {
		if err = o.maybeIgnorePermissionError(osChown(targetPath, int(e.Owner().UserID), int(e.Owner().GroupID))); err != nil {
			return errors.Wrap(err, "could not change owner/group for "+targetPath)
		}
	}

	// Set file permissions from e
	if o.shouldUpdatePermissions(le, e, modclear) {
		if err = o.maybeIgnorePermissionError(osChmod(targetPath, (e.Mode()&fs.ModBits)&^modclear)); err != nil {
			return errors.Wrap(err, "could not change permissions on "+targetPath)
		}
	}

	if o.shouldUpdateTimes(le, e) {
		if err = o.maybeIgnorePermissionError(osChtimes(targetPath, e.ModTime(), e.ModTime())); err != nil {
			return errors.Wrap(err, "could not change mod time on "+targetPath)
		}
	}

	return nil
}

func isSymlink(e fs.Entry) bool {
	_, ok := e.(fs.Symlink)
	return ok
}

func (o *FilesystemOutput) maybeIgnorePermissionError(err error) error {
	if o.IgnorePermissionErrors && os.IsPermission(err) {
		return nil
	}

	return err
}

func (o *FilesystemOutput) shouldUpdateOwner(local, remote fs.Entry) bool {
	if o.SkipOwners {
		return false
	}

	if isWindows() {
		return false
	}

	return local.Owner() != remote.Owner()
}

func (o *FilesystemOutput) shouldUpdatePermissions(local, remote fs.Entry, modclear os.FileMode) bool {
	if o.SkipPermissions {
		return false
	}

	return ((local.Mode() & fs.ModBits) &^ modclear) != (remote.Mode() & fs.ModBits)
}

func (o *FilesystemOutput) shouldUpdateTimes(local, remote fs.Entry) bool {
	if o.SkipTimes {
		return false
	}

	return !local.ModTime().Equal(remote.ModTime())
}

func isWindows() bool {
	return runtime.GOOS == "windows"
}

func (o *FilesystemOutput) createDirectory(ctx context.Context, path string) error {
	switch st, err := os.Stat(path); {
	case os.IsNotExist(err):
		//nolint:wrapcheck
		return os.MkdirAll(path, outputDirMode)
	case err != nil:
		return errors.Wrap(err, "failed to stat path "+path)
	case st.Mode().IsDir():
		if !o.OverwriteDirectories {
			if empty, _ := isEmptyDirectory(path); !empty {
				return errors.Errorf("non-empty directory already exists, not overwriting it: %q", path)
			}
		}

		log(ctx).Debugf("Not creating already existing directory: %v", path)

		return nil
	default:
		return errors.Errorf("unable to create directory, %q already exists and it is not a directory", path)
	}
}

// writeParallel fetches all chunks of pf in parallel and writes them to
// targetPath using WriteAt, allowing out-of-order writes. This avoids the
// sequential round-trip overhead when restoring large files from remote storage.
func writeParallel(ctx context.Context, targetPath string, pf parallelChunkedFile, size int64, workers int, flush bool, progressCb FileWriteProgress) (err error) {
	f, err := os.OpenFile(targetPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600) //nolint:gosec,mnd
	if err != nil {
		return err //nolint:wrapcheck
	}

	defer func() {
		err = stderrors.Join(err, f.Close())
	}()

	if err := f.Truncate(size); err != nil {
		return err //nolint:wrapcheck
	}

	if err := pf.ReadChunksParallel(ctx, workers, func(offset int64, data []byte) error {
		if _, werr := f.WriteAt(data, offset); werr != nil {
			return errors.Wrapf(werr, "writing chunk at offset %v", offset)
		}

		if progressCb != nil {
			progressCb(int64(len(data)))
		}

		return nil
	}); err != nil {
		return err
	}

	if flush {
		if err := f.Sync(); err != nil {
			return errors.Wrapf(err, "cannot flush file %q", f.Name())
		}
	}

	return nil
}

func write(targetPath string, r fs.Reader, size int64, flush bool, c streamCopier) (err error) {
	f, err := os.OpenFile(targetPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600) //nolint:gosec,mnd
	if err != nil {
		return err //nolint:wrapcheck
	}

	defer func() {
		// always close f and report close error
		err = stderrors.Join(err, f.Close())
	}()

	if err := f.Truncate(size); err != nil {
		return err //nolint:wrapcheck
	}

	if _, err := c(f, r); err != nil {
		return errors.Wrapf(err, "cannot write data to file %q", f.Name())
	}

	if flush {
		if err := f.Sync(); err != nil {
			return errors.Wrapf(err, "cannot flush file %q", f.Name())
		}
	}

	return nil
}

func (o *FilesystemOutput) copyFileContent(ctx context.Context, targetPath string, f fs.File, progressCb FileWriteProgress) error {
	switch _, err := os.Stat(targetPath); {
	case os.IsNotExist(err): // copy file below
	case err == nil:
		if !o.OverwriteFiles {
			return errors.Errorf("unable to create %q, it already exists", targetPath)
		}

		log(ctx).Debugf("Overwriting existing file: %v", targetPath)
	default:
		return errors.Wrap(err, "failed to stat "+targetPath)
	}

	log(ctx).Debugf("copying file contents to: %v", targetPath)
	targetPath = atomicfile.MaybePrefixLongFilenameOnWindows(targetPath)

	// Use parallel chunk restore for multi-chunk indirect objects when not using
	// atomic or sparse writes, which require a sequential byte stream.
	if !o.WriteFilesAtomically && !o.WriteSparseFiles {
		if pf, ok := f.(parallelChunkedFile); ok {
			workers := o.ParallelChunkWorkers
			if workers <= 0 {
				workers = object.DefaultParallelChunkWorkers
			}

			err := writeParallel(ctx, targetPath, pf, f.Size(), workers, o.FlushFiles, progressCb)
			if err == nil {
				return nil
			}

			if !errors.Is(err, object.ErrNotParallelizable) {
				return err
			}

			// Single-chunk object: fall through to the sequential path.
		}
	}

	r, err := f.Open(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to open snapshot file for "+targetPath)
	}
	defer r.Close() //nolint:errcheck

	rr := &progressReportingReader{
		Reader: r,
		cb:     progressCb,
	}

	if o.WriteFilesAtomically {
		//nolint:wrapcheck
		return atomicfile.Write(targetPath, rr)
	}

	return write(targetPath, rr, f.Size(), o.FlushFiles, o.copier)
}

func isEmptyDirectory(name string) (bool, error) {
	f, err := os.Open(name) //nolint:gosec
	if err != nil {
		return false, errors.Wrap(err, "error opening directory")
	}

	defer f.Close() //nolint:errcheck

	if _, err = f.Readdirnames(1); errors.Is(err, io.EOF) {
		return true, nil
	}

	return false, errors.Wrap(err, "error reading directory") // Either not empty or error
}

var _ Output = (*FilesystemOutput)(nil)
