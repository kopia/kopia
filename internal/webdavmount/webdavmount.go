package webdavmount

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/kopia/kopia/repo"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/fscache"
	"golang.org/x/net/context"
	"golang.org/x/net/webdav"
)

var _ os.FileInfo = webdavFileInfo{}
var _ webdav.File = (*webdavFile)(nil)
var _ webdav.File = (*webdavDir)(nil)

type webdavFile struct {
	repo.ObjectReader
	entry fs.File
}

func (f *webdavFile) Readdir(n int) ([]os.FileInfo, error) {
	return nil, errors.New("not a directory")
}

func (f *webdavFile) Stat() (os.FileInfo, error) {
	return webdavFileInfo{f.entry.Metadata()}, nil
}

func (f *webdavFile) Write(b []byte) (int, error) {
	return 0, errors.New("read-only filesystem")
}

type webdavDir struct {
	entry fs.Directory
}

func (d *webdavDir) Readdir(n int) ([]os.FileInfo, error) {
	log.Printf("ReadDir(%v)", d.entry.Metadata().Name)
	entries, err := d.entry.Readdir()
	if err != nil {
		return nil, err
	}
	if n > 0 && n < len(entries) {
		entries = entries[0:n]
	}

	var fis []os.FileInfo
	for _, e := range entries {
		fis = append(fis, &webdavFileInfo{e.Metadata()})
	}
	return fis, nil
}

func (d *webdavDir) Stat() (os.FileInfo, error) {
	return webdavFileInfo{d.entry.Metadata()}, nil
}

func (d *webdavDir) Write(b []byte) (int, error) {
	return 0, errors.New("read-only filesystem")
}

func (d *webdavDir) Close() error {
	return nil
}

func (d *webdavDir) Read(b []byte) (int, error) {
	return 0, errors.New("not supported")
}

func (d *webdavDir) Seek(int64, int) (int64, error) {
	return 0, errors.New("not supported")
}

type webdavFileInfo struct {
	md *fs.EntryMetadata
}

func (i webdavFileInfo) IsDir() bool {
	return (i.md.FileMode() & os.ModeDir) != 0
}

func (i webdavFileInfo) ModTime() time.Time {
	return i.md.ModTime
}

func (i webdavFileInfo) Mode() os.FileMode {
	return i.md.FileMode()
}

func (i webdavFileInfo) Name() string {
	return i.md.Name
}

func (i webdavFileInfo) Size() int64 {
	return i.md.FileSize
}

func (i webdavFileInfo) Sys() interface{} {
	return nil
}

type webdavFS struct {
	dir   fs.Directory
	cache *fscache.Cache
}

func (w *webdavFS) Mkdir(ctx context.Context, path string, mode os.FileMode) error {
	return fmt.Errorf("can't create %q: read-only filesystem", path)
}

func (w *webdavFS) RemoveAll(ctx context.Context, path string) error {
	return fmt.Errorf("can't remove %q: read-only filesystem", path)
}

func (w *webdavFS) Rename(ctx context.Context, oldPath, newPath string) error {
	return fmt.Errorf("can't rename %q to %q: read-only filesystem", oldPath, newPath)
}

func (w *webdavFS) OpenFile(ctx context.Context, path string, flags int, mode os.FileMode) (webdav.File, error) {
	f, err := w.findEntry(path)
	if err != nil {
		log.Printf("OpenFile(%q) failed with %v", path, err)
		return nil, err
	}

	switch f := f.(type) {
	case fs.Directory:
		log.Printf("OpenFile(%q) succeeded with directory: %v", path, f.Metadata())
		return &webdavDir{f}, nil
	case fs.File:
		log.Printf("OpenFile(%q) succeeded with file: %v", path, f.Metadata())
		return &webdavFile{nil, f}, nil
	}

	return nil, fmt.Errorf("can't open %q: not implemented", path)
}

func (w *webdavFS) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	e, err := w.findEntry(path)
	if err != nil {
		log.Printf("Stat(%q) failed with %v", path, err)
		return nil, err
	}

	log.Printf("Stat(%q) success with %v", path, e.Metadata())
	return webdavFileInfo{e.Metadata()}, nil
}

func (w *webdavFS) findEntry(path string) (fs.Entry, error) {
	parts := removeEmpty(strings.Split(path, "/"))
	var e fs.Entry = w.dir
	for i, p := range parts {
		d, ok := e.(fs.Directory)
		if !ok {
			return nil, fmt.Errorf("%q not found in %q (not a directory)", p, strings.Join(parts[0:i], "/"))
		}

		entries, err := d.Readdir()
		if err != nil {
			return nil, err
		}

		for _, e := range entries {
			log.Printf("%+v", e.Metadata())
		}

		e = entries.FindByName(p)
		if e == nil {
			return nil, fmt.Errorf("%q not found in %q (not found)", p, strings.Join(parts[0:i], "/"))
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
func WebDAVFS(entry fs.Directory, cache *fscache.Cache) webdav.FileSystem {
	return &webdavFS{entry, cache}
}
