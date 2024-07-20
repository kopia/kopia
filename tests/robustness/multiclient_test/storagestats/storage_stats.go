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
	dd := collectDirectorySizes(dirs)

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
	logFileName := "multiclient_kopia_cache_dir_usage_" + time.Now().UTC().Format("20060102_150405") + ".json" //nolint:forbidigo
	filePath := path.Join(*framework.RepoPathPrefix, logFileSubpath, logFileName)

	return filePath
}

func collectDirectorySizes(dirs []string) []DirectorySize {
	dd := make([]DirectorySize, 0, len(dirs))

	for _, dir := range dirs {
		s, err := getSize(dir)
		if err != nil {
			s = -1

			log.Printf("error getting dir size for '%s' %v", dir, err)
		} else {
			log.Printf("dir: '%s', size: %d", dir, s)
		}

		d := DirectorySize{
			Path: dir,
			Size: s,
		}
		dd = append(dd, d)
	}

	return dd
}
