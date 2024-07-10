//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package storagestats

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"

	"github.com/kopia/kopia/tests/robustness/engine"
)

const (
	generatedDataBaseDirDesc = "Base dir for fio generated data"
	persistDirDesc           = "Dir used to persist metadata about the repo under test"
	checkerRestoreDirDesc    = "Engine checker restore data dir"
	configDirDesc            = "kopia config dir"
	cacheDirDesc             = "kopia cache dir"
	cliLogsDirDesc           = "cli logs dir"
	contentLogsDirDesc       = "content logs dir"
)

// DirDetails ...
type DirDetails struct {
	DirPath string `json:"dirPath,omitempty"`
	DirSize int64  `json:"dirSize,omitempty"`
	Desc    string `json:"desc,omitempty"`
}

// StorageStats ...
type StorageStats struct {
	baseDataDir       DirDetails
	persistDir        DirDetails
	checkerRestoreDir DirDetails
	cfgDir            DirDetails
	kopiaCacheDir     DirDetails
	logsDir           DirDetails
}

// SetupStorageStats ...
func SetupStorageStats(ctx context.Context, eng *engine.Engine) []*DirDetails {
	dirDetails := []*DirDetails{}

	// LocalFioDataPathEnvKey
	dirDetails = append(dirDetails, &DirDetails{
		DirPath: path.Join(eng.FileWriter.DataDirectory(ctx), ".."),
		Desc:    generatedDataBaseDirDesc,
	})

	// kopia-persistence-root-
	dirDetails = append(dirDetails, &DirDetails{
		DirPath: eng.MetaStore.GetPersistDir(),
		Desc:    persistDirDesc,
	})

	// engine-data-*/restore-data-*
	dirDetails = append(dirDetails, &DirDetails{
		DirPath: eng.Checker.RestoreDir,
		Desc:    checkerRestoreDirDesc,
	})

	return dirDetails
}

// LogStorageStats prints memory usage of file writer data dir, test-repo,
// robustness-data and robustness-metadata paths.
func LogStorageStats(ctx context.Context, dd []*DirDetails) {
	log.Printf("Logging storage stats")

	for _, d := range dd {
		dirSize, err := getDirSize(d.DirPath)
		d.DirSize = dirSize
		logDirDetails(d, err)
	}

	// JSON
	jsonData, err := json.Marshal(dd)
	if err != nil {
		log.Printf("Error marshalling to JSON %s", err)
		return
	}

	var ddtmp []DirDetails
	err = json.Unmarshal(jsonData, &ddtmp)
	log.Printf("logging after unmarshal")
	for _, t := range ddtmp {
		log.Printf("dir %s, dir size %d\n", t.DirPath, t.DirSize)
	}

	file, err := os.Create("multiclient_logs.json")
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

func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.WalkDir(path, func(_ string, d os.DirEntry, err error) error {
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

func findDirs(rootPath string) ([]string, error) {
	var dirs []string

	err := filepath.WalkDir(rootPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})
	return dirs, err
}

func catFilesInDir(dirPath string) error {
	err := filepath.WalkDir(dirPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		fmt.Printf("Contents of %s:\n", path)

		reader := bufio.NewReader(file)
		for {
			line, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return err
			}
			fmt.Print(line)
			if err == io.EOF {
				break
			}
		}
		fmt.Println()
		return nil
	})

	return err
}
