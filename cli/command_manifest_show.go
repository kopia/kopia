package cli

import (
	"bytes"
	"fmt"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	manifestShowCommand = manifestCommands.Command("show", "Show manifest items")
	manifestShowItems   = manifestShowCommand.Arg("item", "List of items").Required().Strings()
)

func init() {
	manifestShowCommand.Action(showManifestItems)
}

func showManifestItems(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)

	for _, it := range *manifestShowItems {
		md, err := rep.Manifests.GetMetadata(it)
		if err != nil {
			return fmt.Errorf("error getting metadata for %q: %v", it, err)
		}

		b, err := rep.Manifests.GetRaw(it)
		if err != nil {
			return fmt.Errorf("error showing %q: %v", it, err)
		}

		fmt.Fprintf(os.Stderr, "// id: %v\n", it)
		fmt.Fprintf(os.Stderr, "// length: %v\n", md.Length)
		fmt.Fprintf(os.Stderr, "// modified: %v\n", md.ModTime.Local().Format(timeFormat))
		for k, v := range md.Labels {
			fmt.Fprintf(os.Stderr, "// label %v:%v\n", k, v)
		}
		showContentWithFlags(bytes.NewReader(b), false, true)
	}

	return nil
}
