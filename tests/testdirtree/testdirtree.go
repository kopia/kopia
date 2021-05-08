// Package testdirtree provides utilities for creating test directory trees for testing.
package testdirtree

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/testutil"
)

var globalRandomNameCounter = new(int32)

func intOrDefault(a, b int) int {
	if a > 0 {
		return a
	}

	return b
}

func randomName(opt DirectoryTreeOptions) string {
	maxNameLength := intOrDefault(opt.MaxNameLength, 15)
	minNameLength := intOrDefault(opt.MinNameLength, 3)

	l := rand.Intn(maxNameLength-minNameLength+1) + minNameLength
	b := make([]byte, (l+1)/2)

	cryptorand.Read(b)

	return fmt.Sprintf("%v.%v", hex.EncodeToString(b)[:l], atomic.AddInt32(globalRandomNameCounter, 1))
}

// DirectoryTreeOptions lists options for CreateDirectoryTree.
type DirectoryTreeOptions struct {
	Depth                              int
	MaxSubdirsPerDirectory             int
	MaxFilesPerDirectory               int
	MaxSymlinksPerDirectory            int
	MaxFileSize                        int
	MinNameLength                      int
	MaxNameLength                      int
	NonExistingSymlinkTargetPercentage int // 0..100
}

// MaybeSimplifyFilesystem applies caps to the provided DirectoryTreeOptions to reduce
// test time on ARM.
func MaybeSimplifyFilesystem(o DirectoryTreeOptions) DirectoryTreeOptions {
	if !testutil.ShouldReduceTestComplexity() {
		return o
	}

	if o.Depth > 2 {
		o.Depth = 2
	}

	if o.MaxFilesPerDirectory > 5 {
		o.MaxFilesPerDirectory = 5
	}

	if o.MaxSubdirsPerDirectory > 3 {
		o.MaxFilesPerDirectory = 3
	}

	if o.MaxSymlinksPerDirectory > 3 {
		o.MaxSymlinksPerDirectory = 3
	}

	if o.MaxFileSize > 100000 {
		o.MaxFileSize = 100000
	}

	return o
}

// DirectoryTreeCounters stores stats about files and directories created by CreateDirectoryTree.
type DirectoryTreeCounters struct {
	Files         int
	Directories   int
	Symlinks      int
	TotalFileSize int64
	MaxFileSize   int64
}

// MustCreateDirectoryTree creates a directory tree of a given depth with random files.
func MustCreateDirectoryTree(t *testing.T, dirname string, options DirectoryTreeOptions) {
	t.Helper()

	var counters DirectoryTreeCounters
	if err := createDirectoryTreeInternal(dirname, options, &counters); err != nil {
		t.Fatal(err)
	}

	t.Logf("created directory tree %#v", counters)
}

// CreateDirectoryTree creates a directory tree of a given depth with random files.
func CreateDirectoryTree(dirname string, options DirectoryTreeOptions, counters *DirectoryTreeCounters) error {
	if counters == nil {
		counters = &DirectoryTreeCounters{}
	}

	return createDirectoryTreeInternal(dirname, options, counters)
}

// MustCreateRandomFile creates a new file at the provided path with randomized contents.
// It will fail with a test error if the creation does not succeed.
func MustCreateRandomFile(t *testing.T, filePath string, options DirectoryTreeOptions, counters *DirectoryTreeCounters) {
	t.Helper()

	if err := CreateRandomFile(filePath, options, counters); err != nil {
		t.Fatal(err)
	}
}

// CreateRandomFile creates a new file at the provided path with randomized contents.
func CreateRandomFile(filePath string, options DirectoryTreeOptions, counters *DirectoryTreeCounters) error {
	if counters == nil {
		counters = &DirectoryTreeCounters{}
	}

	return createRandomFile(filePath, options, counters)
}

// createDirectoryTreeInternal creates a directory tree of a given depth with random files.
func createDirectoryTreeInternal(dirname string, options DirectoryTreeOptions, counters *DirectoryTreeCounters) error {
	if err := os.MkdirAll(dirname, 0o700); err != nil {
		return errors.Wrapf(err, "unable to create directory %v", dirname)
	}

	counters.Directories++

	if options.Depth > 0 && options.MaxSubdirsPerDirectory > 0 {
		childOptions := options
		childOptions.Depth--

		numSubDirs := rand.Intn(options.MaxSubdirsPerDirectory) + 1
		for i := 0; i < numSubDirs; i++ {
			subdirName := randomName(options)

			if err := createDirectoryTreeInternal(filepath.Join(dirname, subdirName), childOptions, counters); err != nil {
				return errors.Wrap(err, "unable to create subdirectory")
			}
		}
	}

	var fileNames []string

	if options.MaxFilesPerDirectory > 0 {
		numFiles := rand.Intn(options.MaxFilesPerDirectory) + 1
		for i := 0; i < numFiles; i++ {
			fileName := randomName(options)

			if err := createRandomFile(filepath.Join(dirname, fileName), options, counters); err != nil {
				return errors.Wrap(err, "unable to create random file")
			}

			fileNames = append(fileNames, fileName)
		}
	}

	if options.MaxSymlinksPerDirectory > 0 {
		numSymlinks := rand.Intn(options.MaxSymlinksPerDirectory) + 1
		for i := 0; i < numSymlinks; i++ {
			fileName := randomName(options)

			if err := createRandomSymlink(filepath.Join(dirname, fileName), fileNames, options, counters); err != nil {
				return errors.Wrap(err, "unable to create random symlink")
			}
		}
	}

	return nil
}

func createRandomFile(filename string, options DirectoryTreeOptions, counters *DirectoryTreeCounters) error {
	f, err := os.Create(filename)
	if err != nil {
		return errors.Wrap(err, "unable to create random file")
	}
	defer f.Close()

	maxFileSize := int64(intOrDefault(options.MaxFileSize, 100000))

	length := rand.Int63n(maxFileSize)

	_, err = iocopy.Copy(f, io.LimitReader(rand.New(rand.NewSource(clock.Now().UnixNano())), length))
	if err != nil {
		return errors.Wrap(err, "file create error")
	}

	counters.Files++
	counters.TotalFileSize += length

	if length > counters.MaxFileSize {
		counters.MaxFileSize = length
	}

	return nil
}

func createRandomSymlink(filename string, existingFiles []string, options DirectoryTreeOptions, counters *DirectoryTreeCounters) error {
	counters.Symlinks++

	if len(existingFiles) == 0 || rand.Intn(100) < options.NonExistingSymlinkTargetPercentage {
		return os.Symlink(randomName(options), filename)
	}

	return os.Symlink(existingFiles[rand.Intn(len(existingFiles))], filename)
}
