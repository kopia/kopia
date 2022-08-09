//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

// Package fiofilewriter provides a FileWriter based on FIO.
package fiofilewriter

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"strconv"
	"syscall"

	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/tools/fio"
)

// Option field names.
const (
	DedupePercentStepField       = "dedupe-percent"
	DeletePercentOfContentsField = "delete-contents-percent"
	FreeSpaceLimitField          = "free-space-limit"
	IOLimitPerWriteAction        = "io-limit-per-write"
	MaxDedupePercentField        = "max-dedupe-percent"
	MaxDirDepthField             = "max-dir-depth"
	MaxFileSizeField             = "max-file-size"
	MaxNumFilesPerWriteField     = "max-num-files-per-write"
	MinDedupePercentField        = "min-dedupe-percent"
	MinFileSizeField             = "min-file-size"
	MinNumFilesPerWriteField     = "min-num-files-per-write"
)

// Option defaults.
const (
	defaultDedupePercentStep       = 25
	defaultDeletePercentOfContents = 20
	defaultFreeSpaceLimit          = 100 * 1024 * 1024 // 100 MB
	defaultIOLimitPerWriteAction   = 0                 // A zero value does not impose any limit on IO
	defaultMaxDedupePercent        = 100
	defaultMaxDirDepth             = 20
	defaultMaxFileSize             = 1 * 1024 * 1024 * 1024 // 1GB
	defaultMaxNumFilesPerWrite     = 10000
	defaultMinDedupePercent        = 0
	defaultMinFileSize             = 4096
	defaultMinNumFilesPerWrite     = 1
)

// New returns a FileWriter based on FIO.
// See tests/tools/fio for configuration details.
func New() (*FileWriter, error) {
	runner, err := fio.NewRunner()
	if err != nil {
		return nil, err
	}

	return &FileWriter{Runner: runner}, nil
}

// FileWriter implements a FileWriter over tools/fio.Runner.
type FileWriter struct {
	Runner *fio.Runner
}

var _ robustness.FileWriter = (*FileWriter)(nil)

// DataDirectory returns the data directory configured.
// See tests/tools/fio for details.
func (fw *FileWriter) DataDirectory(ctx context.Context) string {
	return fw.Runner.LocalDataDir
}

// WriteRandomFiles writes a number of files at some filesystem depth, based
// on its input options.
//
//   - MaxDirDepthField
//   - MaxFileSizeField
//   - MinFileSizeField
//   - MaxNumFilesPerWriteField
//   - MinNumFilesPerWriteField
//   - MaxDedupePercentField
//   - MinDedupePercentField
//   - DedupePercentStepField
//
// Default values are used for missing options. The method
// returns the effective options used along with the selected depth
// and the error if any.
func (fw *FileWriter) WriteRandomFiles(ctx context.Context, opts map[string]string) (map[string]string, error) {
	// Directory depth
	maxDirDepth := robustness.GetOptAsIntOrDefault(MaxDirDepthField, opts, defaultMaxDirDepth)
	dirDepth := rand.Intn(maxDirDepth + 1)

	// File size range
	maxFileSizeB := robustness.GetOptAsIntOrDefault(MaxFileSizeField, opts, defaultMaxFileSize)
	minFileSizeB := robustness.GetOptAsIntOrDefault(MinFileSizeField, opts, defaultMinFileSize)

	// Number of files to write
	maxNumFiles := robustness.GetOptAsIntOrDefault(MaxNumFilesPerWriteField, opts, defaultMaxNumFilesPerWrite)
	minNumFiles := robustness.GetOptAsIntOrDefault(MinNumFilesPerWriteField, opts, defaultMinNumFilesPerWrite)

	numFiles := rand.Intn(maxNumFiles-minNumFiles+1) + minNumFiles //nolint:gosec

	// Dedup Percentage
	maxDedupPcnt := robustness.GetOptAsIntOrDefault(MaxDedupePercentField, opts, defaultMaxDedupePercent)
	minDedupPcnt := robustness.GetOptAsIntOrDefault(MinDedupePercentField, opts, defaultMinDedupePercent)

	dedupStep := robustness.GetOptAsIntOrDefault(DedupePercentStepField, opts, defaultDedupePercentStep)

	dedupPcnt := dedupStep * (rand.Intn(maxDedupPcnt/dedupStep-minDedupPcnt/dedupStep+1) + minDedupPcnt/dedupStep) //nolint:gosec

	blockSize := int64(defaultMinFileSize)

	fioOpts := fio.Options{}.
		WithFileSizeRange(int64(minFileSizeB), int64(maxFileSizeB)).
		WithNumFiles(numFiles).
		WithBlockSize(blockSize).
		WithDedupePercentage(dedupPcnt).
		WithNoFallocate()

	ioLimit := robustness.GetOptAsIntOrDefault(IOLimitPerWriteAction, opts, defaultIOLimitPerWriteAction)

	if ioLimit > 0 {
		freeSpaceLimitB := robustness.GetOptAsIntOrDefault(FreeSpaceLimitField, opts, defaultFreeSpaceLimit)

		freeSpaceB, err := getFreeSpaceB(fw.Runner.LocalDataDir)
		if err != nil {
			return nil, err
		}

		log.Printf("Free Space %v B, limit %v B, ioLimit %v B\n", freeSpaceB, freeSpaceLimitB, ioLimit)

		if int(freeSpaceB)-ioLimit < freeSpaceLimitB {
			ioLimit = int(freeSpaceB) - freeSpaceLimitB

			log.Printf("Cutting down I/O limit for space %v", ioLimit)

			if ioLimit <= 0 {
				return nil, robustness.ErrCannotPerformIO
			}
		}

		fioOpts = fioOpts.WithIOLimit(int64(ioLimit))
	}

	relBasePath := "."

	log.Printf("Writing files at depth %v (fileSize: %v-%v, numFiles: %v, blockSize: %v, dedupPcnt: %v, ioLimit: %v)\n", dirDepth, minFileSizeB, maxFileSizeB, numFiles, blockSize, dedupPcnt, ioLimit)

	retOpts := make(map[string]string, len(opts))
	for k, v := range opts {
		retOpts[k] = v
	}

	for k, v := range fioOpts {
		retOpts[k] = v
	}

	retOpts["dirDepth"] = strconv.Itoa(dirDepth)
	retOpts["relBasePath"] = relBasePath

	return retOpts, fw.Runner.WriteFilesAtDepthRandomBranch(relBasePath, dirDepth, fioOpts)
}

// DeleteRandomSubdirectory deletes a random directory up to a specified depth,
// based on its input options:
//
//   - MaxDirDepthField
//
// Default values are used for missing options. The method
// returns the effective options used along with the selected depth
// and the error if any. ErrNoOp is returned if no directory is found.
func (fw *FileWriter) DeleteRandomSubdirectory(ctx context.Context, opts map[string]string) (map[string]string, error) {
	maxDirDepth := robustness.GetOptAsIntOrDefault(MaxDirDepthField, opts, defaultMaxDirDepth)
	if maxDirDepth <= 0 {
		return nil, robustness.ErrInvalidOption
	}

	dirDepth := rand.Intn(maxDirDepth) + 1 //nolint:gosec

	log.Printf("Deleting directory at depth %v\n", dirDepth)

	retOpts := make(map[string]string, len(opts))
	for k, v := range opts {
		retOpts[k] = v
	}

	retOpts["dirDepth"] = strconv.Itoa(dirDepth)

	err := fw.Runner.DeleteDirAtDepth("", dirDepth)
	if errors.Is(err, fio.ErrNoDirFound) {
		log.Print(err)
		err = robustness.ErrNoOp
	}

	return retOpts, err
}

// DeleteDirectoryContents deletes some of the contents of random directory up to a specified depth,
// based on its input options:
//
//   - MaxDirDepthField
//   - DeletePercentOfContentsField
//
// Default values are used for missing options. The method
// returns the effective options used along with the selected depth
// and the error if any. ErrNoOp is returned if no directory is found.
func (fw *FileWriter) DeleteDirectoryContents(ctx context.Context, opts map[string]string) (map[string]string, error) {
	maxDirDepth := robustness.GetOptAsIntOrDefault(MaxDirDepthField, opts, defaultMaxDirDepth)
	dirDepth := rand.Intn(maxDirDepth + 1) //nolint:gosec

	pcnt := robustness.GetOptAsIntOrDefault(DeletePercentOfContentsField, opts, defaultDeletePercentOfContents)

	log.Printf("Deleting %d%% of directory contents at depth %v\n", pcnt, dirDepth)

	retOpts := make(map[string]string, len(opts))
	for k, v := range opts {
		retOpts[k] = v
	}

	retOpts["dirDepth"] = strconv.Itoa(dirDepth)
	retOpts["percent"] = strconv.Itoa(pcnt)

	const pcntConv = 100

	err := fw.Runner.DeleteContentsAtDepth("", dirDepth, float32(pcnt)/pcntConv)
	if errors.Is(err, fio.ErrNoDirFound) {
		log.Print(err)
		err = robustness.ErrNoOp
	}

	return retOpts, err
}

// DeleteEverything deletes all content.
func (fw *FileWriter) DeleteEverything(ctx context.Context) error {
	_, err := fw.DeleteDirectoryContents(ctx, map[string]string{
		MaxDirDepthField:             strconv.Itoa(0),
		DeletePercentOfContentsField: strconv.Itoa(100),
	})

	return err
}

// Cleanup is part of FileWriter.
func (fw *FileWriter) Cleanup() {
	fw.Runner.Cleanup()
}

func getFreeSpaceB(path string) (uint64, error) {
	var stat syscall.Statfs_t

	err := syscall.Statfs(path, &stat)
	if err != nil {
		return 0, err
	}

	// Available blocks * size per block = available space in bytes
	return stat.Bavail * uint64(stat.Bsize), nil
}
