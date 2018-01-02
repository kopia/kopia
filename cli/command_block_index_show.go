package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/kopia/kopia/internal/blockmgrpb"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	blockIndexShowCommand = blockIndexCommands.Command("show", "List block indexes").Alias("cat")
	blockIndexShowIDs     = blockIndexShowCommand.Arg("id", "IDs of index blocks to show").Required().Strings()
)

func runShowBlockIndexesAction(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	for _, blockID := range *blockIndexShowIDs {
		data, err := rep.Blocks.GetBlock(blockID)
		if err != nil {
			return fmt.Errorf("can't read block %q: %v", blockID, err)
		}

		var d blockmgrpb.Indexes
		if err := proto.Unmarshal(data, &d); err != nil {
			return err
		}

		e := json.NewEncoder(os.Stdout)
		e.SetIndent("", "  ")
		e.Encode(&d)
	}

	return nil
}

func init() {
	blockIndexShowCommand.Action(runShowBlockIndexesAction)
}
