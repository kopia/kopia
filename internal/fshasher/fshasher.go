// Package fshasher computes a fingerprint for an FS tree for testing purposes
package fshasher

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"path"
	"sort"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/crypto/blake2s"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("kopia/internal/fshasher")

// Hash computes a recursive hash of e using the given hasher h.
func Hash(ctx context.Context, e fs.Entry) ([]byte, error) {
	h, err := blake2s.New256(nil)
	if err != nil {
		return nil, err
	}

	tw := tar.NewWriter(h)
	defer tw.Close() //nolint:errcheck

	if err := write(ctx, tw, "", e); err != nil {
		return nil, err
	}

	if err := tw.Flush(); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

//nolint:interfacer
func write(ctx context.Context, tw *tar.Writer, fullpath string, e fs.Entry) error {
	h, err := header(ctx, fullpath, e)
	if err != nil {
		return err
	}

	log(ctx).Debugf("%v %v %v %v %v", e.Mode(), h.ModTime.Format(time.RFC3339), h.Size, h.Name, h.Linkname)

	if err := tw.WriteHeader(h); err != nil {
		return err
	}

	switch e := e.(type) {
	case fs.Directory:
		return writeDirectory(ctx, tw, fullpath, e)
	case fs.File:
		return writeFile(ctx, tw, e)
	case fs.Symlink:
		// link target is part of the header
		return nil
	default: // bare fs.Entry
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

	// when hashing, compare time to within a second resolution because of
	// filesystems that don't preserve full timestamp fidelity.
	// https://travis-ci.org/github/kopia/kopia/jobs/732592885
	h.ModTime = h.ModTime.Truncate(time.Second).UTC()
	h.AccessTime = h.ModTime.Truncate(time.Second).UTC()
	h.ChangeTime = h.ModTime.Truncate(time.Second).UTC()

	if sl, ok := e.(fs.Symlink); ok {
		h.Linkname, err = sl.Readlink(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "error reading link")
		}
	}

	return h, nil
}

func writeDirectory(ctx context.Context, tw *tar.Writer, fullpath string, d fs.Directory) error {
	all, err := fs.GetAllEntries(ctx, d)
	if err != nil {
		return errors.Wrap(err, "error getting all entries")
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name() < all[j].Name()
	})

	for _, e := range all {
		if err2 := write(ctx, tw, path.Join(fullpath, e.Name()), e); err2 != nil {
			return err2
		}
	}

	return err
}

func writeFile(ctx context.Context, w io.Writer, f fs.File) error {
	r, err := f.Open(ctx)
	if err != nil {
		return err
	}
	defer r.Close() //nolint:errcheck

	return iocopy.JustCopy(w, r)
}
