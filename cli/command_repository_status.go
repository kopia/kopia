package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"reflect"

	"github.com/kopia/kopia/internal/scrubber"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
)

var (
	statusCommand = repositoryCommands.Command("status", "Display the status of connected repository.")
)

func runStatusCommand(ctx context.Context, rep *repo.Repository) error {
	fmt.Printf("Config file:         %v\n", rep.ConfigFile)
	fileCount, totalFileSize, err := scanCacheDir(filepath.Join(rep.CacheDirectory, "blocks"))
	if err != nil {
		fmt.Printf("Cache directory:     %v (error: %v)\n", rep.CacheDirectory, err)
	} else {
		fmt.Printf("Cache directory:     %v (%v files, %v)\n", rep.CacheDirectory, fileCount, units.BytesStringBase2(totalFileSize))
	}
	fmt.Println()

	ci := rep.Storage.ConnectionInfo()
	fmt.Printf("Storage type:        %v\n", ci.Type)

	if cjson, err := json.MarshalIndent(scrubber.ScrubSensitiveData(reflect.ValueOf(ci.Config)).Interface(), "                     ", "  "); err == nil {
		fmt.Printf("Storage config:      %v\n", string(cjson))
	}
	fmt.Println()

	fmt.Println()
	fmt.Printf("Unique ID:           %x\n", rep.UniqueID)
	fmt.Println()
	fmt.Printf("Block hash:          %v\n", rep.Blocks.Format.Hash)
	fmt.Printf("Block encryption:    %v\n", rep.Blocks.Format.Encryption)
	fmt.Printf("Max pack length:     %v\n", units.BytesStringBase2(int64(rep.Blocks.Format.MaxPackSize)))
	fmt.Printf("Splitter:            %v\n", rep.Objects.Format.Splitter)

	return nil
}

func scanCacheDir(dirname string) (fileCount int, totalFileLength int64, err error) {
	entries, err := ioutil.ReadDir(dirname)
	if err != nil {
		return 0, 0, err
	}

	for _, e := range entries {
		if e.IsDir() {
			subdir := filepath.Join(dirname, e.Name())
			c, l, err2 := scanCacheDir(subdir)
			if err2 != nil {
				return 0, 0, err2
			}
			fileCount += c
			totalFileLength += l
			continue
		}

		fileCount++
		totalFileLength += e.Size()
	}

	return
}

func init() {
	statusCommand.Action(repositoryAction(runStatusCommand))
}
