// Package cachefs implements a wrapper that caches filesystem actions.
package cachefs

import (
	"github.com/kopia/kopia/fs"
)

// DirectoryCacher reads and potentially caches directory entries for a given directory.
type DirectoryCacher interface {
	Readdir(d fs.Directory) (fs.Entries, error)
}

type context struct {
	cacher DirectoryCacher
}

type directory struct {
	ctx *context
	fs.Directory
}

func (d *directory) Readdir() (fs.Entries, error) {
	entries, err := d.ctx.cacher.Readdir(d.Directory)
	if err != nil {
		return entries, err
	}

	wrapped := make(fs.Entries, len(entries))
	for i, entry := range entries {
		wrapped[i] = wrapWithContext(entry, d.ctx)
	}
	return wrapped, err
}

type file struct {
	ctx *context
	fs.File
}

type symlink struct {
	ctx *context
	fs.Symlink
}

// Wrap returns an Entry that wraps another Entry and caches directory reads.
func Wrap(e fs.Entry, cacher DirectoryCacher) fs.Entry {
	return wrapWithContext(e, &context{cacher})
}

func wrapWithContext(e fs.Entry, opts *context) fs.Entry {
	switch e := e.(type) {
	case fs.Directory:
		return fs.Directory(&directory{opts, e})

	case fs.File:
		return fs.File(&file{opts, e})

	case fs.Symlink:
		return fs.Symlink(&symlink{opts, e})

	default:
		return e
	}
}

var _ fs.Directory = &directory{}
var _ fs.File = &file{}
var _ fs.Symlink = &symlink{}
