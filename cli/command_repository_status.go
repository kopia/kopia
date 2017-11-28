package cli

import (
	"encoding/json"
	"fmt"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/storage"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	statusCommand = repositoryCommands.Command("status", "Display the status of connected repository.")
)

func runStatusCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	fmt.Printf("Config file:         %v\n", rep.ConfigFile)
	fmt.Printf("Cache directory:     %v\n", rep.CacheDirectory)
	fmt.Println()

	if cip, ok := rep.Storage.(storage.ConnectionInfoProvider); ok {
		ci := cip.ConnectionInfo()
		fmt.Printf("Storage type:        %v\n", ci.Type)
		if cjson, err := json.MarshalIndent(ci.Config, "                     ", "  "); err == nil {
			fmt.Printf("Storage config:      %v\n", string(cjson))
		}
		fmt.Println()
	}

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
	fmt.Printf("Max packed len:      %v\n", units.BytesStringBase2(int64(rep.Blocks.Format.MaxPackedContentLength)))
	fmt.Printf("Max pack length:     %v\n", units.BytesStringBase2(int64(rep.Blocks.Format.MaxPackSize)))
	fmt.Printf("Splitter:            %v%v\n", rep.Objects.Format.Splitter, splitterExtraInfo)

	return nil
}

func init() {
	statusCommand.Action(runStatusCommand)
}
