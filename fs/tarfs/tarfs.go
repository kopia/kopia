// Package tarfs exposes the contents of a tar archive as an fs.Directory tree,
// so that the files inside an archive can be snapshotted individually.
package tarfs

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("tarfs")

const dirPermissions os.FileMode = 0o777

const gzipMagicLen = 2

var (
	errSparseUnsupported = errors.New("sparse tar entries are not supported")
	errHardLinkTarget    = errors.New("hard link target is not a regular file")
	errPathConflict      = errors.New("archive entry conflicts with an earlier entry of a different type")
)

// Directory is the root of a tar archive.
type Directory interface {
	fs.Directory

	// Close releases the spooled archive. Safe to call more than once.
	Close()
}

// NewDirectory indexes the tar archive read from r (plain or gzip-compressed)
// and returns a directory named name with the archive's entries as children.
//
// The archive is spooled to a temporary file so that files can be read in any
// order and more than once; the file is removed by Close().
func NewDirectory(ctx context.Context, name string, r io.Reader) (Directory, error) {
	spool, err := os.CreateTemp("", "kopia-tar-*")
	if err != nil {
		return nil, errors.Wrap(err, "unable to create spool file")
	}

	root, err := indexArchive(ctx, name, r, spool)
	if err != nil {
		removeSpool(ctx, spool)

		return nil, err
	}

	return root, nil
}

func removeSpool(ctx context.Context, spool *os.File) {
	spool.Close() //nolint:errcheck

	if err := os.Remove(spool.Name()); err != nil && !os.IsNotExist(err) {
		log(ctx).Warnf("unable to remove tar spool file %v: %v", spool.Name(), err)
	}
}

func indexArchive(ctx context.Context, name string, r io.Reader, spool *os.File) (*rootDirectory, error) {
	br := bufio.NewReader(r)

	magic, err := br.Peek(gzipMagicLen)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read archive")
	}

	var src io.Reader = br

	if magic[0] == 0x1f && magic[1] == 0x8b {
		gz, gzErr := gzip.NewReader(br)
		if gzErr != nil {
			return nil, errors.Wrap(gzErr, "unable to read gzip archive")
		}

		src = gz
	}

	// tee everything the tar reader consumes into the spool and count it, so
	// that the offset of each entry's data is the count right after Next().
	counter := &countingWriter{w: spool}
	tr := tar.NewReader(io.TeeReader(src, counter))

	root := &rootDirectory{
		directory: newDir(name, dirPermissions|os.ModeDir, time.Time{}, fs.OwnerInfo{}),
		spool:     spool,
	}
	root.root = root

	for {
		if err := ctx.Err(); err != nil {
			return nil, errors.Wrap(err, "indexing canceled")
		}

		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, counter.wrap(err, "unable to read tar header")
		}

		if err := root.add(ctx, hdr, counter.n); err != nil {
			return nil, err
		}

		// skip the data so the tee spools it and the counter advances past it.
		// The whole archive lands on disk by design, so there is no bound to apply here.
		if _, err := io.Copy(io.Discard, tr); err != nil { //nolint:gosec
			return nil, counter.wrap(err, "unable to read tar data")
		}
	}

	// drain any trailing bytes (tar padding) so the spool is complete.
	if _, err := io.Copy(io.Discard, io.TeeReader(src, counter)); err != nil {
		return nil, counter.wrap(err, "unable to read archive trailer")
	}

	return root, nil
}

// countingWriter counts the bytes written to the spool and remembers the
// first write failure, so a full temp directory is reported as such rather
// than as a read error from the tar reader.
type countingWriter struct {
	w   io.Writer
	n   int64
	err error
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)

	if err != nil && c.err == nil {
		c.err = err
	}

	return n, err //nolint:wrapcheck
}

func (c *countingWriter) wrap(err error, msg string) error {
	if c.err != nil {
		return errors.Wrap(c.err, "unable to spool archive to a temporary file")
	}

	return errors.Wrap(err, msg)
}

// entry holds the metadata shared by every kind of archive entry.
type entry struct {
	name    string
	mode    os.FileMode
	size    int64
	modTime time.Time
	owner   fs.OwnerInfo
}

func (e *entry) Name() string                { return e.name }
func (e *entry) IsDir() bool                 { return e.mode.IsDir() }
func (e *entry) Mode() os.FileMode           { return e.mode }
func (e *entry) ModTime() time.Time          { return e.modTime }
func (e *entry) Size() int64                 { return e.size }
func (e *entry) Sys() any                    { return nil }
func (e *entry) Owner() fs.OwnerInfo         { return e.owner }
func (e *entry) Device() fs.DeviceInfo       { return fs.DeviceInfo{} }
func (e *entry) LocalFilesystemPath() string { return "" }
func (e *entry) Close()                      {}

type directory struct {
	entry

	root     *rootDirectory
	children map[string]fs.Entry
	sorted   []fs.Entry // built lazily once the archive is fully indexed
	once     sync.Once
}

func newDir(name string, mode os.FileMode, modTime time.Time, owner fs.OwnerInfo) *directory {
	return &directory{
		entry:    entry{name: name, mode: mode, modTime: modTime, owner: owner},
		children: map[string]fs.Entry{},
	}
}

func (d *directory) entries() []fs.Entry {
	d.once.Do(func() {
		d.sorted = make([]fs.Entry, 0, len(d.children))
		for _, c := range d.children {
			d.sorted = append(d.sorted, c)
		}

		sort.Slice(d.sorted, func(i, j int) bool { return d.sorted[i].Name() < d.sorted[j].Name() })
	})

	return d.sorted
}

func (d *directory) Child(_ context.Context, name string) (fs.Entry, error) {
	if c, ok := d.children[name]; ok {
		return c, nil
	}

	return nil, fs.ErrEntryNotFound
}

func (d *directory) Iterate(_ context.Context) (fs.DirectoryIterator, error) {
	return fs.StaticIterator(append([]fs.Entry{}, d.entries()...), nil), nil
}

func (d *directory) SupportsMultipleIterations() bool { return true }

// dirFor returns the directory at the given clean archive path, creating any
// missing directories along the way (archives may omit directory headers).
// A non-directory entry already at any point of the path is an error: the
// snapshot could not represent such an archive.
func (d *directory) dirFor(p string) (*directory, error) {
	cur := d

	if p == "." || p == "" {
		return cur, nil
	}

	walked := ""

	for part := range strings.SplitSeq(p, "/") {
		walked = path.Join(walked, part)

		next, ok := cur.children[part]
		if !ok {
			nd := newDir(part, dirPermissions|os.ModeDir, time.Time{}, fs.OwnerInfo{})
			nd.root = d.root
			cur.children[part] = nd
			cur = nd

			continue
		}

		nd, ok := next.(*directory)
		if !ok {
			return nil, errors.Wrap(errPathConflict, walked)
		}

		cur = nd
	}

	return cur, nil
}

type rootDirectory struct {
	*directory

	spool     *os.File
	closeOnce sync.Once
}

func (r *rootDirectory) Close() {
	r.closeOnce.Do(func() {
		removeSpool(context.Background(), r.spool)
	})
}

func (r *rootDirectory) add(ctx context.Context, hdr *tar.Header, dataOffset int64) error {
	// PAX headers describe the next entry (or the whole archive, as git
	// archive's pax_global_header does) and are not entries themselves.
	if hdr.Typeflag == tar.TypeXHeader || hdr.Typeflag == tar.TypeXGlobalHeader {
		return nil
	}

	clean := path.Clean("/" + hdr.Name)
	if clean == "/" {
		return nil // the archive root itself
	}

	clean = strings.TrimPrefix(clean, "/")

	if path.IsAbs(hdr.Name) || escapesRoot(hdr.Name) {
		log(ctx).Warnf("tar entry %q stored as %q", hdr.Name, clean)
	}

	dirName, base := path.Split(clean)

	parent, err := r.dirFor(strings.TrimSuffix(dirName, "/"))
	if err != nil {
		return err
	}

	if existing, ok := parent.children[base]; ok && existing.IsDir() && hdr.Typeflag != tar.TypeDir {
		return errors.Wrap(errPathConflict, clean)
	}

	fi := hdr.FileInfo()
	meta := entry{
		name:    base,
		mode:    fi.Mode(),
		size:    hdr.Size,
		modTime: hdr.ModTime,
		owner:   fs.OwnerInfo{UserID: uint32(hdr.Uid), GroupID: uint32(hdr.Gid)}, //nolint:gosec
	}

	return r.addEntry(ctx, parent, clean, meta, hdr, dataOffset)
}

func (r *rootDirectory) addEntry(ctx context.Context, parent *directory, clean string, meta entry, hdr *tar.Header, dataOffset int64) error {
	base := meta.name

	switch hdr.Typeflag {
	case tar.TypeDir:
		d, err := parent.dirFor(base)
		if err != nil {
			return err
		}

		d.entry = meta

	case tar.TypeReg:
		if isSparse(hdr) {
			return errors.Wrap(errSparseUnsupported, clean)
		}

		parent.children[base] = &file{entry: meta, root: r, offset: dataOffset}

	case tar.TypeLink:
		// a hard link whose target is not a regular file in this archive
		// (docker layers link across layers, for instance) is reported for
		// that one entry rather than failing the whole archive.
		tf, err := r.lookupFile(hdr.Linkname)
		if err != nil {
			parent.children[base] = &errorEntry{entry: meta, err: errors.Wrapf(err, "hard link %q -> %q", clean, hdr.Linkname)}

			return nil
		}

		meta.size = tf.size
		parent.children[base] = &file{entry: meta, root: r, offset: tf.offset}

	case tar.TypeSymlink:
		parent.children[base] = &symlink{entry: meta, parent: parent, target: hdr.Linkname}

	case tar.TypeGNUSparse:
		return errors.Wrap(errSparseUnsupported, clean)

	default:
		log(ctx).Debugf("unsupported tar entry %q (type %q)", clean, hdr.Typeflag)

		parent.children[base] = &errorEntry{entry: meta, err: fs.ErrUnknown}
	}

	return nil
}

func (r *rootDirectory) lookupFile(p string) (*file, error) {
	target, err := r.lookup(p)
	if err != nil {
		return nil, err
	}

	tf, ok := target.(*file)
	if !ok {
		return nil, errHardLinkTarget
	}

	return tf, nil
}

// escapesRoot reports whether the name has a ".." segment.
func escapesRoot(name string) bool {
	for part := range strings.SplitSeq(name, "/") {
		if part == ".." {
			return true
		}
	}

	return false
}

func isSparse(hdr *tar.Header) bool {
	for k := range hdr.PAXRecords {
		if strings.HasPrefix(k, "GNU.sparse.") {
			return true
		}
	}

	return false
}

// lookup finds an entry by its clean archive path.
func (r *rootDirectory) lookup(p string) (fs.Entry, error) {
	clean := strings.TrimPrefix(path.Clean("/"+p), "/")
	if clean == "" {
		return r, nil
	}

	var cur fs.Entry = r.directory

	for part := range strings.SplitSeq(clean, "/") {
		d, ok := cur.(*directory)
		if !ok {
			return nil, fs.ErrEntryNotFound
		}

		next, ok := d.children[part]
		if !ok {
			return nil, fs.ErrEntryNotFound
		}

		cur = next
	}

	return cur, nil
}

type file struct {
	entry

	root   *rootDirectory
	offset int64
}

func (f *file) Open(_ context.Context) (fs.Reader, error) {
	return &fileReader{
		SectionReader: io.NewSectionReader(f.root.spool, f.offset, f.size),
		f:             f,
	}, nil
}

type fileReader struct {
	*io.SectionReader

	f *file
}

func (r *fileReader) Close() error { return nil }

func (r *fileReader) Entry() (fs.Entry, error) { return r.f, nil }

type symlink struct {
	entry

	parent *directory
	target string
}

func (s *symlink) Readlink(_ context.Context) (string, error) { return s.target, nil }

// Resolve follows the link inside the archive. Absolute targets are resolved
// from the archive root; relative ones from the link's own directory.
func (s *symlink) Resolve(_ context.Context) (fs.Entry, error) {
	if path.IsAbs(s.target) {
		return s.parent.root.lookup(s.target)
	}

	return s.parent.root.lookup(path.Join(s.parent.pathFromRoot(), s.target))
}

// pathFromRoot walks the parent chain; directories are few, so a linear
// search from the root is fine.
func (d *directory) pathFromRoot() string {
	var walk func(cur *directory, prefix string) (string, bool)

	walk = func(cur *directory, prefix string) (string, bool) {
		if cur == d {
			return prefix, true
		}

		for name, c := range cur.children {
			if cd, ok := c.(*directory); ok {
				if p, found := walk(cd, path.Join(prefix, name)); found {
					return p, true
				}
			}
		}

		return "", false
	}

	p, _ := walk(d.root.directory, "")

	return p
}

type errorEntry struct {
	entry

	err error
}

func (e *errorEntry) ErrorInfo() error { return e.err }

var (
	_ fs.Directory  = (*directory)(nil)
	_ Directory     = (*rootDirectory)(nil)
	_ fs.File       = (*file)(nil)
	_ fs.Reader     = (*fileReader)(nil)
	_ fs.Symlink    = (*symlink)(nil)
	_ fs.ErrorEntry = (*errorEntry)(nil)
)
