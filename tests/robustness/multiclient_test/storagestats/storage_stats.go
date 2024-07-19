//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

// Package storagestats contains logging mechanism
// log disk space consumed by directories created by
// robustness test framework before and after the test run.
package storagestats

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/kopia/kopia/tests/robustness/multiclient_test/framework"
)

const (
	logFileSubpath = "logs"
)

var logFilePath string

// DirectorySize represents details about a directory,
// path, and size.
type DirectorySize struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// LogStorageStats logs disk space usage of provided dir paths.
func LogStorageStats(ctx context.Context, dirs []string) error {
	dd := collectDirectorySize(dirs)

	// write dir details into a JSON file
	jsonData, err := json.Marshal(dd)
	if err != nil {
		return fmt.Errorf("error marshaling to JSON: %w", err)
	}

	logFilePath = getLogFilePath()
	log.Printf("log file path %s", logFilePath)
	err = os.WriteFile(logFilePath, jsonData, 0o644)
	if err != nil {
		return fmt.Errorf("error writing log file: %w", err)
	}

	return nil
}

func logDirectorySize(dd DirectorySize, err error) {
	if err != nil {
		log.Printf("error when getting dir size for %s %v", dd.Path, err)
		return
	}
	log.Printf("dir %s, dir size %d\n", dd.Path, dd.Size)
}

func getSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.WalkDir(dirPath, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// skip
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func getLogFilePath() string {
	logFileName := fmt.Sprint("multiclient_kopia_cache_dir_usage_", time.Now().UTC().Format("20060102_150405"), ".json") //nolint:forbidigo
	filePath := path.Join(*framework.RepoPathPrefix, logFileSubpath, logFileName)
	return filePath
}

func collectDirectorySize(dirs []string) []DirectorySize {
	var dd []DirectorySize
	for _, dir := range dirs {
		Size, err := getSize(dir)
		if err != nil {
			Size = -1
		}
		d := DirectorySize{
			Path: dir,
			Size: Size,
		}
		dd = append(dd, d)
		// Useful if JSON marshaling errors out later.
		logDirectorySize(d, err)
	}

	return dd
}
