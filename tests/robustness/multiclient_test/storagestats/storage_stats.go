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

	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/robustness/multiclient_test/framework"
)

const (
	generatedDataBaseDirDesc = "Base dir for fio generated data"
	persistDirDesc           = "Dir used to persist metadata about the repo under test"
	checkerRestoreDirDesc    = "Engine checker restore data dir"
)

var logFilePath string

// DirDetails represents details about a directory,
// path, size and description.
type DirDetails struct {
	DirPath string `json:"dirPath"`
	DirSize int64  `json:"dirSize"`
	Desc    string `json:"desc"`
}

// SetupStorageStats populates the directory details to be logged later.
func SetupStorageStats(ctx context.Context, eng *engine.Engine) []*DirDetails {
	dirDetails := []*DirDetails{}

	dirDetails = append(dirDetails,
		// LocalFioDataPathEnvKey
		&DirDetails{
			DirPath: path.Join(eng.FileWriter.DataDirectory(ctx), ".."),
			Desc:    generatedDataBaseDirDesc,
		},
		// kopia-persistence-root-
		&DirDetails{
			DirPath: eng.MetaStore.GetPersistDir(),
			Desc:    persistDirDesc,
		},
		// engine-data-*/restore-data-*
		&DirDetails{
			DirPath: eng.Checker.RestoreDir,
			Desc:    checkerRestoreDirDesc,
		})

	return dirDetails
}

// LogStorageStats logs disk space usage of file writer data dir, test-repo,
// robustness-data and robustness-metadata paths.
func LogStorageStats(ctx context.Context, dd []*DirDetails) {
	if logFilePath == "" {
		logFilePath = getLogFilePath()
		log.Printf("log file path %s", logFilePath)
	}
	log.Printf("Logging storage stats")

	for _, d := range dd {
		dirSize, err := getDirSize(d.DirPath)
		d.DirSize = dirSize
		logDirDetails(d, err)
	}

	// write logs into a JSON file
	jsonData, err := json.Marshal(dd)
	if err != nil {
		log.Printf("Error marshaling to JSON %s", err)
		return
	}

	file, err := os.Create(logFilePath)
	if err != nil {
		log.Printf("Error creating file %s", err)
		return
	}
	defer file.Close()

	_, err = file.Write(jsonData)
	if err != nil {
		log.Printf("Error writing to file %s", err)
		return
	}
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
	logFileSubpath := fmt.Sprint("multiclient_logs_", time.Now().UTC(), ".json") //nolint:forbidigo
	filePath := path.Join(*framework.RepoPathPrefix, logFileSubpath)
	log.Printf("filepath %s", filePath)
	return filePath
}
