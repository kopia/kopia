package fio

import (
	"log"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// WriteFiles writes files to the directory specified by path, up to the
// provided size and number of files.
func (fr *Runner) WriteFiles(relPath string, opt Options) error {
	lock, err := fr.PathLock.Lock(relPath)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	fullPath := filepath.Join(fr.LocalDataDir, relPath)

	return fr.writeFiles(fullPath, opt)
}

func (fr *Runner) writeFiles(fullPath string, opt Options) error {
	err := os.MkdirAll(fullPath, 0o700)
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
// the base data directory.
func (fr *Runner) WriteFilesAtDepth(relBasePath string, depth int, opt Options) error {
	lock, err := fr.PathLock.Lock(relBasePath)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	fullBasePath := filepath.Join(fr.LocalDataDir, relBasePath)

	err = os.MkdirAll(fullBasePath, 0o700)
	if err != nil {
		return errors.Wrapf(err, "unable to make base dir %v for writing at depth", fullBasePath)
	}

	return fr.writeFilesAtDepth(fullBasePath, depth, depth, opt)
}

// WriteFilesAtDepthRandomBranch writes files to a directory "depth" layers deep below
// the base data directory and branches at a random depth.
func (fr *Runner) WriteFilesAtDepthRandomBranch(relBasePath string, depth int, opt Options) error {
	lock, err := fr.PathLock.Lock(relBasePath)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	fullBasePath := filepath.Join(fr.LocalDataDir, relBasePath)

	err = os.MkdirAll(fullBasePath, 0o700)
	if err != nil {
		return errors.Wrapf(err, "unable to make base dir %v for writing at depth with a branch", fullBasePath)
	}

	return fr.writeFilesAtDepth(fullBasePath, depth, rand.Intn(depth+1), opt)
}

// DeleteRelDir deletes a relative directory in the runner's data directory.
func (fr *Runner) DeleteRelDir(relDirPath string) error {
	lock, err := fr.PathLock.Lock(relDirPath)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	return os.RemoveAll(filepath.Join(fr.LocalDataDir, relDirPath))
}

// DeleteDirAtDepth deletes a random directory at the given depth.
func (fr *Runner) DeleteDirAtDepth(relBasePath string, depth int) error {
	lock, err := fr.PathLock.Lock(relBasePath)
	if err != nil {
		log.Printf("could not lock dir to delete")
		return err
	}
	defer lock.Unlock()

	log.Printf("locked " + relBasePath)

	if depth == 0 {
		return ErrCanNotDeleteRoot
	}

	fullBasePath := filepath.Join(fr.LocalDataDir, relBasePath)

	return fr.operateAtDepth(fullBasePath, depth, os.RemoveAll)
}

// DeleteContentsAtDepth deletes some or all of the contents of a directory
// at the provided depths. The probability argument denotes the probability in the
// range [0.0,1.0] that a given file system entry in the directory at this depth will be
// deleted. Probability set to 0 will delete nothing. Probability set to 1 will delete
// everything.
func (fr *Runner) DeleteContentsAtDepth(relBasePath string, depth int, prob float32) error {
	lock, err := fr.PathLock.Lock(relBasePath)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	fullBasePath := filepath.Join(fr.LocalDataDir, relBasePath)

	log.Printf("locked " + fullBasePath)

	return fr.operateAtDepth(fullBasePath, depth, func(dirPath string) error {
		dirEntries, err := os.ReadDir(dirPath)
		if err != nil {
			return err
		}

		for _, entry := range dirEntries {
			if rand.Float32() < prob {
				path := filepath.Join(dirPath, entry.Name())
				err = os.RemoveAll(path)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}

// List of known errors.
var (
	ErrNoDirFound       = errors.New("no directory found at this depth")
	ErrCanNotDeleteRoot = errors.New("can not delete root directory")
)

func (fr *Runner) operateAtDepth(path string, depth int, f func(string) error) error {
	if depth <= 0 {
		log.Printf("performing operation on directory %s\n", path)
		return f(path)
	}

	dirEntries, err := os.ReadDir(path)
	if err != nil {
		return errors.Wrapf(err, "unable to read dir at path %v", path)
	}

	log.Printf("read path " + path)

	var dirList []string

	for _, entry := range dirEntries {
		if entry.IsDir() {
			dirList = append(dirList, filepath.Join(path, entry.Name()))
			log.Printf("adding dir " + entry.Name())
		}
	}

	rand.Shuffle(len(dirList), func(i, j int) {
		dirList[i], dirList[j] = dirList[j], dirList[i]
	})

	for _, dirName := range dirList {
		err = fr.operateAtDepth(dirName, depth-1, f)
		if !errors.Is(err, ErrNoDirFound) {
			return err
		}
	}

	log.Printf("erroring out after recursion")

	return ErrNoDirFound
}

// writeFilesAtDepth traverses the file system tree until it reaches a given "depth", at which
// point it uses fio to write one or more files controlled by the provided Options.
// The "branchDepth" argument gives control over whether the files will be written into
// existing directories or a new path entirely. The function will traverse existing directories
// (if any) until it reaches "branchDepth", after which it will begin creating new directories
// for the remainder of the path until "depth" is reached.
// If "branchDepth" is zero, the entire path will be newly created directories.
// If "branchDepth" is greater than or equal to "depth", the files will be written to
// a random existing directory, if one exists at that depth.
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
		subdirPath, err = os.MkdirTemp(fromDirPath, "dir_")
		if err != nil {
			return errors.Wrapf(err, "unable to create temp dir at %v", fromDirPath)
		}
	}

	return fr.writeFilesAtDepth(subdirPath, depth-1, branchDepth-1, opt)
}

func pickRandSubdirPath(dirPath string) (subdirPath string) {
	dirEntries, err := os.ReadDir(dirPath)
	if err != nil {
		return ""
	}

	// Find all entries that are directories, record each of their dirEntries indexes
	dirIdxs := make([]int, 0, len(dirEntries))

	for idx, entry := range dirEntries {
		if entry.IsDir() {
			dirIdxs = append(dirIdxs, idx)
		}
	}

	if len(dirIdxs) == 0 {
		return ""
	}

	// Pick a random index from the list of indexes of DirEntry entries known to be directories.
	randDirIdx := dirIdxs[rand.Intn(len(dirIdxs))] //nolint:gosec
	randDirInfo := dirEntries[randDirIdx]

	return filepath.Join(dirPath, randDirInfo.Name())
}
