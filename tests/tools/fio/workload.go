package fio

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// WriteFiles writes files to the directory specified by path, up to the
// provided size and number of files
func (fr *Runner) WriteFiles(relPath string, opt Options) error {
	fullPath := filepath.Join(fr.LocalDataDir, relPath)
	return fr.writeFiles(fullPath, opt)
}

func (fr *Runner) writeFiles(fullPath string, opt Options) error {
	err := os.MkdirAll(fullPath, 0700)
	if err != nil {
		return errors.Wrap(err, "unable to make directory for write")
	}

	relWritePath, err := filepath.Rel(fr.LocalDataDir, fullPath)
	if err != nil {
		return errors.Wrapf(err, "error finding relative file path between %v and %v", fr.LocalDataDir, fullPath)
	}

	absWritePath := filepath.Join(fr.FioWriteBaseDir, relWritePath)

	_, _, err = fr.RunConfigs(Config{
		{
			Name: "writeFiles",
			Options: opt.Merge(
				Options{
					"readwrite":       RandWriteFio,
					"filename_format": "file_$filenum",
				}.WithDirectory(absWritePath),
			),
		},
	})

	return err
}

// DeleteRelDir deletes a relative directory in the runner's data directory
func (fr *Runner) DeleteRelDir(relDirPath string) error {
	return os.RemoveAll(filepath.Join(fr.LocalDataDir, relDirPath))
}
