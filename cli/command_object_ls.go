package cli

import (
	"fmt"
	"strings"

	"github.com/kopia/kopia/repo"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	objectListCommand = objectCommands.Command("list", "List objects").Alias("ls")
	objectListPrefix  = objectListCommand.Flag("prefix", "Prefix").String()
)

func runListObjectsAction(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	ch, cancel := rep.Storage.ListBlocks(*objectListPrefix)
	defer cancel()

	for b := range ch {
		if b.Error != nil {
			return b.Error
		}

		if strings.HasPrefix(b.BlockID, repo.MetadataBlockPrefix) {
			continue
		}

		fmt.Printf("D%-34v %10v %v\n", b.BlockID, b.Length, b.TimeStamp.Local().Format(timeFormat))
	}

	return nil
}

func init() {
	objectListCommand.Action(runListObjectsAction)
}
