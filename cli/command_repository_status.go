package cli

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"

	"github.com/kopia/kopia/internal/scrubber"
	"github.com/kopia/kopia/internal/units"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	statusCommand = repositoryCommands.Command("status", "Display the status of connected repository.")
)

func runStatusCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	fmt.Printf("Config file:         %v\n", rep.ConfigFile)
	entries, err := ioutil.ReadDir(rep.CacheDirectory)
	if err != nil {
	}
	if err != nil {
		fmt.Printf("Cache directory:     %v (error: %v)\n", rep.CacheDirectory, err)
	} else {
		var totalSize int64
		for _, e := range entries {
			totalSize += e.Size()
		}
		fmt.Printf("Cache directory:     %v (%v files, %v)\n", rep.CacheDirectory, len(entries), units.BytesStringBase2(totalSize))
	}
	fmt.Println()

	ci := rep.Storage.ConnectionInfo()
	fmt.Printf("Storage type:        %v\n", ci.Type)

	if cjson, err := json.MarshalIndent(scrubber.ScrubSensitiveData(reflect.ValueOf(ci.Config)).Interface(), "                     ", "  "); err == nil {
		fmt.Printf("Storage config:      %v\n", string(cjson))
	}
	fmt.Println()

	var splitterExtraInfo string

	switch rep.Objects.Format.Splitter {
	case "DYNAMIC":
		splitterExtraInfo = fmt.Sprintf(
			" (min: %v; avg: %v; max: %v)",
			units.BytesStringBase2(int64(rep.Objects.Format.MinBlockSize)),
			units.BytesStringBase2(int64(rep.Objects.Format.AvgBlockSize)),
			units.BytesStringBase2(int64(rep.Objects.Format.MaxBlockSize)))
	case "":
	case "FIXED":
		splitterExtraInfo = fmt.Sprintf(" %v", units.BytesStringBase2(int64(rep.Objects.Format.MaxBlockSize)))
	}

	fmt.Println()
	fmt.Printf("Key Derivation:      %v\n", rep.Security.KeyDerivationAlgorithm)
	fmt.Printf("Unique ID:           %x\n", rep.Security.UniqueID)
	fmt.Println()
	fmt.Printf("Object manager:      v%v\n", rep.Objects.Format.Version)
	fmt.Printf("Block format:        %v\n", rep.Blocks.Format.BlockFormat)
	fmt.Printf("Max pack length:     %v\n", units.BytesStringBase2(int64(rep.Blocks.Format.MaxPackSize)))
	fmt.Printf("Splitter:            %v%v\n", rep.Objects.Format.Splitter, splitterExtraInfo)

	return nil
}

func init() {
	statusCommand.Action(runStatusCommand)
}
