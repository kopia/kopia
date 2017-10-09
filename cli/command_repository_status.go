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

	s := rep.Status()

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

	switch s.Splitter {
	case "DYNAMIC":
		splitterExtraInfo = fmt.Sprintf(
			" (min: %v; avg: %v; max: %v)",
			units.BytesStringBase2(int64(s.MinBlockSize)),
			units.BytesStringBase2(int64(s.AvgBlockSize)),
			units.BytesStringBase2(int64(s.MaxBlockSize)))
	case "":
	case "FIXED":
		splitterExtraInfo = fmt.Sprintf(" %v", units.BytesStringBase2(int64(s.MaxBlockSize)))
	}

	fmt.Println()
	fmt.Printf("Metadata manager:    v%v\n", s.MetadataManagerVersion)
	fmt.Printf("Metadata Encryption: %v\n", s.MetadataEncryptionAlgorithm)
	fmt.Printf("Key Derivation:      %v\n", s.KeyDerivationAlgorithm)
	fmt.Printf("Unique ID:           %v\n", s.UniqueID)
	fmt.Println()
	fmt.Printf("Object manager:      v%v\n", s.ObjectManagerVersion)
	fmt.Printf("Block format:        %v\n", s.BlockFormat)
	fmt.Printf("Splitter:            %v%v\n", s.Splitter, splitterExtraInfo)
	fmt.Printf("Inline content len:  %v\n", s.MaxInlineContentLength)
	fmt.Printf("Max packed len:      %v\n", units.BytesStringBase2(int64(s.MaxPackedContentLength)))

	return nil
}

func init() {
	statusCommand.Action(runStatusCommand)
}
