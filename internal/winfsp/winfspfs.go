//go:build windows && winfsp && cgo

// Package winfsp implements a read-only FUSE filesystem for Kopia snapshots using WinFsp via cgofuse.
package winfsp

import (
	"context"
	"io"
	"os"
	"strings"
	"sync"

	cgofuse "github.com/winfsp/cgofuse/fuse"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("winfsp")

// KopiaFS implements cgofuse.FileSystemInterface for read-only access to Kopia snapshots.
type KopiaFS struct {
	cgofuse.FileSystemBase

	root fs.Directory

	mu         sync.Mutex
	handles    map[uint64]fs.Reader
	nextHandle uint64
}

// NewKopiaFS creates a new WinFsp filesystem backed by the given Kopia directory.
func NewKopiaFS(root fs.Directory) *KopiaFS {
	return &KopiaFS{
		root:       root,
		handles:    make(map[uint64]fs.Reader),
		nextHandle: 1,
	}
}

func (k *KopiaFS) allocHandle(reader fs.Reader) uint64 {
	k.mu.Lock()
	defer k.mu.Unlock()

	fh := k.nextHandle
	k.nextHandle++
	k.handles[fh] = reader

	return fh
}

func (k *KopiaFS) getHandle(fh uint64) fs.Reader {
	k.mu.Lock()
	defer k.mu.Unlock()

	return k.handles[fh]
}

func (k *KopiaFS) releaseHandle(fh uint64) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if r, ok := k.handles[fh]; ok {
		r.Close() //nolint:errcheck
		delete(k.handles, fh)
	}
}

func (k *KopiaFS) lookup(path string) (fs.Entry, error) {
	path = strings.ReplaceAll(path, "\\", "/")

	if path == "/" || path == "" {
		return k.root, nil
	}

	parts := strings.Split(strings.Trim(path, "/"), "/")

	var current fs.Entry = k.root

	for _, part := range parts {
		if part == "" {
			continue
		}

		dir, ok := current.(fs.Directory)
		if !ok {
			return nil, os.ErrNotExist
		}

		child, err := dir.Child(context.Background(), part)
		if err != nil || child == nil {
			return nil, os.ErrNotExist
		}

		current = child
	}

	return current, nil
}

func populateStat(stat *cgofuse.Stat_t, entry fs.Entry) {
	stat.Size = entry.Size()
	stat.Mtim = cgofuse.NewTimespec(entry.ModTime())
	stat.Atim = stat.Mtim
	stat.Ctim = stat.Mtim
	stat.Nlink = 1
	stat.Uid = entry.Owner().UserID
	stat.Gid = entry.Owner().GroupID

	switch entry.(type) {
	case fs.Directory:
		stat.Mode = cgofuse.S_IFDIR | 0o555
	case fs.Symlink:
		stat.Mode = cgofuse.S_IFLNK | 0o444
	default:
		stat.Mode = cgofuse.S_IFREG | 0o444
	}
}

// Getattr returns file attributes.
func (k *KopiaFS) Getattr(path string, stat *cgofuse.Stat_t, _ uint64) int {
	entry, err := k.lookup(path)
	if err != nil {
		return -cgofuse.ENOENT
	}

	populateStat(stat, entry)

	return 0
}

// Open opens a file for reading. The filesystem is read-only — any open
// requesting write access (O_WRONLY, O_RDWR, O_APPEND, O_TRUNC, O_CREAT)
// is rejected with EROFS so Windows clients that probe writability fail
// fast at open time rather than discovering it on a later write.
func (k *KopiaFS) Open(path string, flags int) (int, uint64) {
	const writeMask = os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_TRUNC | os.O_CREATE
	if flags&writeMask != 0 {
		return -cgofuse.EROFS, ^uint64(0)
	}

	entry, err := k.lookup(path)
	if err != nil {
		return -cgofuse.ENOENT, ^uint64(0)
	}

	file, ok := entry.(fs.File)
	if !ok {
		return -cgofuse.EISDIR, ^uint64(0)
	}

	reader, err := file.Open(context.Background())
	if err != nil {
		log(context.Background()).Errorf("error opening %v: %v", path, err)
		return -cgofuse.EIO, ^uint64(0)
	}

	fh := k.allocHandle(reader)

	return 0, fh
}

// Read reads data from an open file.
func (k *KopiaFS) Read(path string, buff []byte, ofst int64, fh uint64) int {
	reader := k.getHandle(fh)
	if reader == nil {
		return -cgofuse.EBADF
	}

	if _, err := reader.Seek(ofst, io.SeekStart); err != nil {
		log(context.Background()).Errorf("seek error: %v offset=%v: %v", path, ofst, err)
		return -cgofuse.EIO
	}

	n, err := reader.Read(buff)
	if err != nil && err != io.EOF {
		log(context.Background()).Errorf("read error: %v: %v", path, err)
		return -cgofuse.EIO
	}

	return n
}

// Release closes a file handle.
func (k *KopiaFS) Release(_ string, fh uint64) int {
	k.releaseHandle(fh)
	return 0
}

// Opendir opens a directory for reading.
func (k *KopiaFS) Opendir(path string) (int, uint64) {
	entry, err := k.lookup(path)
	if err != nil {
		return -cgofuse.ENOENT, ^uint64(0)
	}

	if _, ok := entry.(fs.Directory); !ok {
		return -cgofuse.ENOTDIR, ^uint64(0)
	}

	return 0, 0
}

// Readdir reads directory entries.
func (k *KopiaFS) Readdir(path string,
	fill func(name string, stat *cgofuse.Stat_t, ofst int64) bool,
	_ int64,
	_ uint64,
) int {
	entry, err := k.lookup(path)
	if err != nil {
		return -cgofuse.ENOENT
	}

	dir, ok := entry.(fs.Directory)
	if !ok {
		return -cgofuse.ENOTDIR
	}

	fill(".", nil, 0)
	fill("..", nil, 0)

	iter, err := dir.Iterate(context.Background())
	if err != nil {
		log(context.Background()).Errorf("error reading directory %v: %v", path, err)
		return -cgofuse.EIO
	}

	defer iter.Close()

	cur, err := iter.Next(context.Background())
	for cur != nil {
		var stat cgofuse.Stat_t

		populateStat(&stat, cur)

		if !fill(cur.Name(), &stat, 0) {
			break
		}

		cur, err = iter.Next(context.Background())
	}

	if err != nil {
		log(context.Background()).Errorf("error iterating directory %v: %v", path, err)
		return -cgofuse.EIO
	}

	return 0
}

// Readlink reads the target of a symbolic link.
func (k *KopiaFS) Readlink(path string) (int, string) {
	entry, err := k.lookup(path)
	if err != nil {
		return -cgofuse.ENOENT, ""
	}

	symlink, ok := entry.(fs.Symlink)
	if !ok {
		return -cgofuse.EINVAL, ""
	}

	target, err := symlink.Readlink(context.Background())
	if err != nil {
		log(context.Background()).Errorf("error reading symlink %v: %v", path, err)
		return -cgofuse.EIO, ""
	}

	return 0, target
}

// Statfs returns filesystem statistics.
func (k *KopiaFS) Statfs(_ string, stat *cgofuse.Statfs_t) int {
	stat.Bsize = 4096  //nolint:mnd
	stat.Frsize = 4096 //nolint:mnd
	stat.Namemax = 255 //nolint:mnd

	return 0
}

// Destroy cleans up all open handles.
func (k *KopiaFS) Destroy() {
	k.mu.Lock()
	defer k.mu.Unlock()

	for fh, r := range k.handles {
		r.Close() //nolint:errcheck
		delete(k.handles, fh)
	}
}
