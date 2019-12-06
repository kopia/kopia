// Package fshasher computes a fingerprint for an FS tree for testing purposes
package fshasher

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"path"
	"time"

	"golang.org/x/crypto/blake2s"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/repologging"
)

var log = repologging.Logger("kopia/internal/fshasher")

// Hash computes a recursive hash of e using the given hasher h
func Hash(ctx context.Context, e fs.Entry) ([]byte, error) {
	h, err := blake2s.New256(nil)
	if err != nil {
		return nil, err
	}

	tw := tar.NewWriter(h)
	defer tw.Close()

	if err := write(ctx, tw, "", e); err != nil {
		return nil, err
	}

	if err := tw.Flush(); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// nolint:interfacer
func write(ctx context.Context, tw *tar.Writer, fullpath string, e fs.Entry) error {
	h, err := header(ctx, fullpath, e)
	if err != nil {
		return err
	}

	log.Debug(e.Mode(), h.ModTime.Format(time.RFC3339), h.Size, h.Name)

	if err := tw.WriteHeader(h); err != nil {
		return err
	}

	switch e := e.(type) {
	case fs.Directory:
		return writeDirectory(ctx, tw, fullpath, e)
	case fs.File:
		return writeFile(ctx, tw, e)
	default: // fs.Symlink or bare fs.Entry
		return nil
	}
}

func header(ctx context.Context, fullpath string, e os.FileInfo) (*tar.Header, error) {
	var link string

	if sl, ok := e.(fs.Symlink); ok {
		l, err := sl.Readlink(ctx)
		if err != nil {
			return nil, err
		}

		link = l
	}

	h, err := tar.FileInfoHeader(e, link)
	if err != nil {
		return nil, err
	}

	h.Name = fullpath

	// clear fields that may cause spurious differences
	if e.IsDir() {
		// reset times for directories given how ModTime is set in
		// snapshot directories
		h.ModTime = time.Time{}
	}

	h.ModTime = h.ModTime.UTC()
	h.AccessTime = h.ModTime
	h.ChangeTime = h.ModTime

	return h, nil
}

func writeDirectory(ctx context.Context, tw *tar.Writer, fullpath string, d fs.Directory) error {
	entries, err := d.Readdir(ctx)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if err := write(ctx, tw, path.Join(fullpath, e.Name()), e); err != nil {
			return err
		}
	}

	return nil
}

func writeFile(ctx context.Context, w io.Writer, f fs.File) error {
	r, err := f.Open(ctx)
	if err != nil {
		return err
	}
	defer r.Close()

	_, err = io.Copy(w, r)

	return err
}
