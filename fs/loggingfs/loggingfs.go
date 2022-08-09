// Package loggingfs implements a wrapper that logs all filesystem actions.
package loggingfs

import (
	"context"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/timetrack"
)

type loggingOptions struct {
	printf func(fmt string, args ...interface{})
	prefix string
}

type loggingDirectory struct {
	relativePath string
	options      *loggingOptions
	fs.Directory
}

func (ld *loggingDirectory) Child(ctx context.Context, name string) (fs.Entry, error) {
	timer := timetrack.StartTimer()
	entry, err := ld.Directory.Child(ctx, name)
	dt := timer.Elapsed()
	ld.options.printf(ld.options.prefix+"Child(%v) took %v and returned %v", ld.relativePath, dt, err)

	if err != nil {
		//nolint:wrapcheck
		return nil, err
	}

	return wrapWithOptions(entry, ld.options, ld.relativePath+"/"+entry.Name()), nil
}

func (ld *loggingDirectory) IterateEntries(ctx context.Context, callback func(context.Context, fs.Entry) error) error {
	timer := timetrack.StartTimer()
	entries, err := fs.GetAllEntries(ctx, ld.Directory)
	dt := timer.Elapsed()
	ld.options.printf(ld.options.prefix+"Readdir(%v) took %v and returned %v items", ld.relativePath, dt, len(entries))

	if err != nil {
		return err
	}

	for _, e := range entries {
		if err2 := callback(ctx, wrapWithOptions(e, ld.options, ld.relativePath+"/"+e.Name())); err2 != nil {
			return err2
		}
	}

	return nil
}

type loggingFile struct {
	options *loggingOptions
	fs.File
}

type loggingSymlink struct {
	options *loggingOptions
	fs.Symlink
}

// Option modifies the behavior of logging wrapper.
type Option func(o *loggingOptions)

// Wrap returns an Entry that wraps another Entry and logs all method calls.
func Wrap(e fs.Entry, printf func(msg string, args ...interface{}), options ...Option) fs.Entry {
	return wrapWithOptions(e, applyOptions(printf, options), ".")
}

func wrapWithOptions(e fs.Entry, opts *loggingOptions, relativePath string) fs.Entry {
	switch e := e.(type) {
	case fs.Directory:
		return fs.Directory(&loggingDirectory{relativePath, opts, e})

	case fs.File:
		return fs.File(&loggingFile{opts, e})

	case fs.Symlink:
		return fs.Symlink(&loggingSymlink{opts, e})

	default:
		return e
	}
}

func applyOptions(printf func(msg string, args ...interface{}), opts []Option) *loggingOptions {
	o := &loggingOptions{
		printf: printf,
	}

	for _, f := range opts {
		f(o)
	}

	return o
}

// Output is an option that causes all output to be sent to a given function instead of log.Printf().
func Output(outputFunc func(fmt string, args ...interface{})) Option {
	return func(o *loggingOptions) {
		o.printf = outputFunc
	}
}

// Prefix specifies prefix to be prepended to all log output.
func Prefix(prefix string) Option {
	return func(o *loggingOptions) {
		o.prefix = prefix
	}
}

var (
	_ fs.Directory = &loggingDirectory{}
	_ fs.File      = &loggingFile{}
	_ fs.Symlink   = &loggingSymlink{}
)
