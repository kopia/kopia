// Package webdavmount implements webdav filesystem for serving snapshots.
package webdavmount

import (
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/net/webdav"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("kopia/webdavmount")

var (
	_ os.FileInfo = webdavFileInfo{}
	_ webdav.File = (*webdavFile)(nil)
	_ webdav.File = (*webdavDir)(nil)
)

type webdavFile struct {
	// webdavFile implements webdav.File but needs context
	// +checklocks:mu
	ctx context.Context //nolint:containedctx

	entry fs.File

	mu sync.Mutex

	// +checklocks:mu
	r fs.Reader
}

func (f *webdavFile) Readdir(_ int) ([]os.FileInfo, error) {
	return nil, errors.New("not a directory")
}

func (f *webdavFile) Stat() (os.FileInfo, error) {
	return webdavFileInfo{f.entry}, nil
}

func (f *webdavFile) getReader() (fs.Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.r == nil {
		r, err := f.entry.Open(f.ctx)
		if err != nil {
			return nil, errors.Wrap(err, "error opening webdav file")
		}

		f.r = r
	}

	return f.r, nil
}

func (f *webdavFile) Read(b []byte) (int, error) {
	r, err := f.getReader()
	if err != nil {
		return 0, err
	}

	//nolint:wrapcheck
	return r.Read(b)
}

func (f *webdavFile) Seek(offset int64, whence int) (int64, error) {
	r, err := f.getReader()
	if err != nil {
		return 0, err
	}

	//nolint:wrapcheck
	return r.Seek(offset, whence)
}

func (f *webdavFile) Write(_ []byte) (int, error) {
	return 0, errors.New("read-only filesystem")
}

func (f *webdavFile) Close() error {
	f.mu.Lock()
	r := f.r
	f.r = nil
	f.mu.Unlock()

	if r != nil {
		//nolint:wrapcheck
		return r.Close()
	}

	return nil
}

type webdavDir struct {
	// webdavDir implements webdav.File but needs context
	ctx context.Context //nolint:containedctx

	w    *webdavFS
	info os.FileInfo
	iter fs.DirectoryIterator
}

//nolint:gochecknoglobals
var symlinksAreUnsupportedLogged = new(int32)

func (d *webdavDir) Readdir(n int) ([]os.FileInfo, error) {
	ctx := d.ctx

	var fis []os.FileInfo

	foundEntries := 0

	e, err := d.iter.Next(ctx)
	for e != nil {
		if n > 0 && foundEntries >= n {
			break
		}

		foundEntries++

		if _, isSymlink := e.(fs.Symlink); isSymlink {
			if atomic.AddInt32(symlinksAreUnsupportedLogged, 1) == 1 {
				log(d.ctx).Errorf("Mounting directories containing symbolic links using WebDAV is not supported. The link entries will be skipped.")
			}
		} else {
			fis = append(fis, &webdavFileInfo{e})
		}

		e, err = d.iter.Next(ctx)
	}

	if err != nil {
		return nil, errors.Wrap(err, "error reading directory")
	}

	return fis, nil
}

func (d *webdavDir) Stat() (os.FileInfo, error) {
	return d.info, nil
}

func (d *webdavDir) Write(_ []byte) (int, error) {
	return 0, errors.New("read-only filesystem")
}

func (d *webdavDir) Close() error {
	d.iter.Close()
	return nil
}

func (d *webdavDir) Read(_ []byte) (int, error) {
	return 0, errors.New("not supported")
}

func (d *webdavDir) Seek(int64, int) (int64, error) {
	return 0, errors.New("not supported")
}

type webdavFileInfo struct {
	fs.Entry
}

type webdavFS struct {
	dir fs.Directory
}

func (w *webdavFS) Mkdir(ctx context.Context, path string, _ os.FileMode) error {
	return errors.Errorf("can't create %q: read-only filesystem", path)
}

func (w *webdavFS) RemoveAll(ctx context.Context, path string) error {
	return errors.Errorf("can't remove %q: read-only filesystem", path)
}

func (w *webdavFS) Rename(ctx context.Context, oldPath, newPath string) error {
	return errors.Errorf("can't rename %q to %q: read-only filesystem", oldPath, newPath)
}

func (w *webdavFS) OpenFile(ctx context.Context, path string, _ int, _ os.FileMode) (webdav.File, error) {
	f, err := w.findEntry(ctx, path)
	if err != nil {
		log(ctx).Errorf("OpenFile(%q) failed with %v", path, err)
		return nil, err
	}

	switch f := f.(type) {
	case fs.Directory:
		iter, err := f.Iterate(ctx)
		if err != nil {
			return nil, err //nolint:wrapcheck
		}

		return &webdavDir{ctx, w, webdavFileInfo{f}, iter}, nil
	case fs.File:
		return &webdavFile{ctx: ctx, entry: f}, nil
	}

	return nil, errors.Errorf("can't open %q: not implemented", path)
}

func (w *webdavFS) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	e, err := w.findEntry(ctx, path)
	if err != nil {
		return nil, err
	}

	return webdavFileInfo{e}, nil
}

func (w *webdavFS) findEntry(ctx context.Context, path string) (fs.Entry, error) {
	parts := removeEmpty(strings.Split(path, "/"))

	var e fs.Entry = w.dir

	for i, p := range parts {
		d, ok := e.(fs.Directory)
		if !ok {
			return nil, errors.Errorf("%q not found in %q (not a directory)", p, strings.Join(parts[0:i], "/"))
		}

		var err error

		e, err = d.Child(ctx, p)
		if err != nil {
			return nil, errors.Wrap(err, "error reading directory")
		}

		if e == nil {
			return nil, errors.Errorf("%q not found in %q (not found)", p, strings.Join(parts[0:i], "/"))
		}
	}

	return e, nil
}

func removeEmpty(s []string) []string {
	result := s[:0]

	for _, e := range s {
		if e == "" {
			continue
		}

		result = append(result, e)
	}

	return result
}

// WebDAVFS returns a webdav.FileSystem implementation for a given directory.
func WebDAVFS(entry fs.Directory) webdav.FileSystem {
	return &webdavFS{entry}
}
