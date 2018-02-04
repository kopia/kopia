package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	storageShowCommand  = storageCommands.Command("show", "Show storage blocks").Alias("cat")
	storageShowBlockIDs = storageShowCommand.Arg("blockIDs", "Block IDs").Required().Strings()
)

func runShowStorageBlocks(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	for _, b := range *storageShowBlockIDs {
		d, err := rep.Storage.GetBlock(b, 0, -1)
		if err != nil {
			return fmt.Errorf("error getting %v: %v", b, err)
		}
		io.Copy(os.Stdout, bytes.NewReader(d))
	}

	return nil
}

func init() {
	storageShowCommand.Action(runShowStorageBlocks)
}
