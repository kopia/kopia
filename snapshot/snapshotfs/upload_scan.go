package snapshotfs

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

type scanResults struct {
	numFiles      int
	totalFileSize int64
}

// scanDirectory computes the number of files and their total size in a given directory recursively descending
// into subdirectories. The scan teminates early as soon as the provided context is canceled.
func (u *Uploader) scanDirectory(ctx context.Context, dir fs.Directory) (scanResults, error) {
	var res scanResults

	if u.disableEstimation {
		return res, nil
	}

	entries, err := dir.Readdir(ctx)
	if err != nil {
		return res, errors.Wrap(err, "unable to read directory")
	}

	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			// terminate early if context got canceled
			// nolint:wrapcheck
			return res, err
		}

		switch e := e.(type) {
		case fs.Directory:
			dr, err := u.scanDirectory(ctx, e)
			res.numFiles += dr.numFiles
			res.totalFileSize += dr.totalFileSize

			if err != nil {
				return res, err
			}

		case fs.File:
			res.numFiles++
			res.totalFileSize += e.Size()
		}
	}

	return res, nil
}
