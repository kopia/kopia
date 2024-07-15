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

// DirDetails represents details about a directory,
// path, and size.
type DirDetails struct {
	DirPath string `json:"dirPath"`
	DirSize int64  `json:"dirSize"`
}

// LogStorageStats logs disk space usage of provided dir paths.
func LogStorageStats(ctx context.Context, dirs []string) error {
	dd := collectDirDetails(dirs)

	// write dir details into a JSON file
	jsonData, err := json.Marshal(dd)
	if err != nil {
		return fmt.Errorf("error marshaling to JSON: %w", err)
	}

	file, err := createLogFile()
	if err != nil {
		return fmt.Errorf("error creating log file: %w", err)
	}
	defer file.Close()

	_, err = file.Write(jsonData)
	if err != nil {
		return fmt.Errorf("error writing to log file: %w", err)
	}

	return nil
}

func logDirDetails(dd *DirDetails, err error) {
	if err != nil {
		log.Printf("error when getting dir size for %s %v", dd.DirPath, err)
		return
	}
	log.Printf("dir %s, dir size %d\n", dd.DirPath, dd.DirSize)
}

func getDirSize(dirPath string) (int64, error) {
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
	logFileName := fmt.Sprint("multiclient_logs_", time.Now().UTC().Format("20060102_150405"), ".json") //nolint:forbidigo
	filePath := path.Join(*framework.RepoPathPrefix, logFileSubpath, logFileName)
	return filePath
}

func collectDirDetails(dirs []string) []*DirDetails {
	var dd []*DirDetails
	for _, dir := range dirs {
		dirSize, err := getDirSize(dir)
		if err != nil {
			dirSize = -1
		}
		d := &DirDetails{
			DirPath: dir,
			DirSize: dirSize,
		}
		dd = append(dd, d)
		// Useful if JSON marshaling errors out later.
		logDirDetails(d, err)
	}

	return dd
}

func createLogFile() (*os.File, error) {
	// Create a fresh log file.
	logFilePath = getLogFilePath()
	log.Printf("log file path %s", logFilePath)
	file, err := os.Create(logFilePath)

	return file, err
}
