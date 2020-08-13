package fio

import (
	"io/ioutil"
	"log"
	"math/rand"
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

// WriteFilesAtDepth writes files to a directory "depth" layers deep below
// the base data directory
func (fr *Runner) WriteFilesAtDepth(relBasePath string, depth int, opt Options) error {
	fullBasePath := filepath.Join(fr.LocalDataDir, relBasePath)

	err := os.MkdirAll(fullBasePath, 0700)
	if err != nil {
		return errors.Wrapf(err, "unable to make base dir %v for writing at depth", fullBasePath)
	}

	return fr.writeFilesAtDepth(fullBasePath, depth, depth, opt)
}

// WriteFilesAtDepthRandomBranch writes files to a directory "depth" layers deep below
// the base data directory and branches at a random depth
func (fr *Runner) WriteFilesAtDepthRandomBranch(relBasePath string, depth int, opt Options) error {
	fullBasePath := filepath.Join(fr.LocalDataDir, relBasePath)

	err := os.MkdirAll(fullBasePath, 0700)
	if err != nil {
		return errors.Wrapf(err, "unable to make base dir %v for writing at depth with a branch", fullBasePath)
	}

	return fr.writeFilesAtDepth(fullBasePath, depth, rand.Intn(depth+1), opt)
}

// DeleteRelDir deletes a relative directory in the runner's data directory
func (fr *Runner) DeleteRelDir(relDirPath string) error {
	return os.RemoveAll(filepath.Join(fr.LocalDataDir, relDirPath))
}

// DeleteDirAtDepth deletes a random directory at the given depth
func (fr *Runner) DeleteDirAtDepth(relBasePath string, depth int) error {
	if depth == 0 {
		return ErrCanNotDeleteRoot
	}

	fullBasePath := filepath.Join(fr.LocalDataDir, relBasePath)

	return fr.operateAtDepth(fullBasePath, depth, os.RemoveAll)
}

// DeleteContentsAtDepth deletes some or all of the contents of a directory
// at the provided depths
func (fr *Runner) DeleteContentsAtDepth(relBasePath string, depth, pcnt int) error {
	fullBasePath := filepath.Join(fr.LocalDataDir, relBasePath)

	return fr.operateAtDepth(fullBasePath, depth, func(dirPath string) error {
		fileInfoList, err := ioutil.ReadDir(dirPath)
		if err != nil {
			return err
		}

		for _, fi := range fileInfoList {
			const hundred = 100
			if rand.Intn(hundred) < pcnt {
				path := filepath.Join(dirPath, fi.Name())
				err = os.RemoveAll(path)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}

// List of known errors
var (
	ErrNoDirFound       = errors.New("no directory found at this depth")
	ErrCanNotDeleteRoot = errors.New("can not delete root directory")
)

func (fr *Runner) operateAtDepth(path string, depth int, f func(string) error) error {
	if depth <= 0 {
		log.Printf("performing operation on directory %s\n", path)
		return f(path)
	}

	fileInfoList, err := ioutil.ReadDir(path)
	if err != nil {
		return errors.Wrapf(err, "unable to read dir at path %v", path)
	}

	var dirList []string

	for _, fi := range fileInfoList {
		if fi.IsDir() {
			dirList = append(dirList, filepath.Join(path, fi.Name()))
		}
	}

	rand.Shuffle(len(dirList), func(i, j int) {
		dirList[i], dirList[j] = dirList[j], dirList[i]
	})

	for _, dirName := range dirList {
		err = fr.operateAtDepth(dirName, depth-1, f)
		if err != ErrNoDirFound {
			return err
		}
	}

	return ErrNoDirFound
}

func (fr *Runner) writeFilesAtDepth(fromDirPath string, depth, branchDepth int, opt Options) error {
	if depth <= 0 {
		return fr.writeFiles(fromDirPath, opt)
	}

	var subdirPath string

	if branchDepth > 0 {
		subdirPath = pickRandSubdirPath(fromDirPath)
	}

	if subdirPath == "" {
		var err error

		// Couldn't find a subdir, create one instead
		subdirPath, err = ioutil.TempDir(fromDirPath, "dir_")
		if err != nil {
			return errors.Wrapf(err, "unable to create temp dir at %v", fromDirPath)
		}
	}

	return fr.writeFilesAtDepth(subdirPath, depth-1, branchDepth-1, opt)
}

func pickRandSubdirPath(dirPath string) (subdirPath string) {
	subdirCount := 0

	fileInfoList, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return ""
	}

	for _, fi := range fileInfoList {
		if fi.IsDir() {
			subdirCount++

			// Decide if this directory will be selected - probability of
			// being selected is uniform across all subdirs
			if rand.Intn(subdirCount) == 0 {
				subdirPath = filepath.Join(dirPath, fi.Name())
			}
		}
	}

	return subdirPath
}
