package localfs

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

type filesystemDirectoryIterator struct {
	dirHandle   *os.File
	childPrefix string

	currentIndex int
	currentBatch []os.DirEntry
}

func (it *filesystemDirectoryIterator) Next(ctx context.Context) (fs.Entry, error) {
	for {
		// we're at the end of the current batch, fetch the next batch
		if it.currentIndex >= len(it.currentBatch) {
			batch, err := it.dirHandle.ReadDir(numEntriesToRead)
			if err != nil && !errors.Is(err, io.EOF) {
				// stop iteration
				return nil, err //nolint:wrapcheck
			}

			it.currentIndex = 0
			it.currentBatch = batch

			// got empty batch
			if len(batch) == 0 {
				return nil, nil
			}
		}

		n := it.currentIndex
		it.currentIndex++

		e, err := toDirEntryOrNil(it.currentBatch[n], it.childPrefix)
		if err != nil {
			// stop iteration
			return nil, err
		}

		if e == nil {
			// go to the next item
			continue
		}

		return e, nil
	}
}

func (it *filesystemDirectoryIterator) Close() {
	it.dirHandle.Close() //nolint:errcheck
}

func (fsd *filesystemDirectory) Iterate(ctx context.Context) (fs.DirectoryIterator, error) {
	fullPath := fsd.fullPath()

	f, direrr := os.Open(fullPath) //nolint:gosec
	if direrr != nil {
		return nil, errors.Wrap(direrr, "unable to read directory")
	}

	childPrefix := fullPath + string(filepath.Separator)

	return &filesystemDirectoryIterator{dirHandle: f, childPrefix: childPrefix}, nil
}
