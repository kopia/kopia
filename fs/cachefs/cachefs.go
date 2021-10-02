// Package cachefs implements a wrapper that caches filesystem actions.
package cachefs

import (
	"context"

	"github.com/kopia/kopia/fs"
)

// DirectoryCacher reads and potentially caches directory entries for a given directory.
type DirectoryCacher interface {
	Readdir(ctx context.Context, d fs.Directory, w EntryWrapper) (fs.Entries, error)
}

type cacheContext struct {
	cacher DirectoryCacher
}

type directory struct {
	ctx *cacheContext
	fs.Directory
}

func (d *directory) Child(ctx context.Context, name string) (fs.Entry, error) {
	e, err := d.Directory.Child(ctx, name)
	if err != nil {
		// nolint:wrapcheck
		return nil, err
	}

	return wrapWithContext(e, d.ctx), nil
}

func (d *directory) Readdir(ctx context.Context) (fs.Entries, error) {
	entries, err := d.ctx.cacher.Readdir(ctx, d.Directory, func(e fs.Entry) fs.Entry {
		return wrapWithContext(e, d.ctx)
	})
	if err != nil {
		// nolint:wrapcheck
		return entries, err
	}

	// nolint:wrapcheck
	return entries, err
}

type file struct {
	ctx *cacheContext
	fs.File
}

type symlink struct {
	ctx *cacheContext
	fs.Symlink
}

// Wrap returns an Entry that wraps another Entry and caches directory reads.
func Wrap(e fs.Entry, cacher DirectoryCacher) fs.Entry {
	return wrapWithContext(e, &cacheContext{cacher})
}

func wrapWithContext(e fs.Entry, opts *cacheContext) fs.Entry {
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

var (
	_ fs.Directory = &directory{}
	_ fs.File      = &file{}
	_ fs.Symlink   = &symlink{}
)
