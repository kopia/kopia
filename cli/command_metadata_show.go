package cli

import (
	"bytes"
	"encoding/json"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	metadataShowCommand   = metadataCommands.Command("show", "Show contents of a metadata item").Alias("cat").Hidden()
	metadataShowID        = metadataShowCommand.Arg("id", "ID of the metadata item to show").String()
	metadataShowJSON      = metadataShowCommand.Flag("json", "Pretty-print JSON").Short('j').Bool()
	metadataShowNoNewLine = metadataShowCommand.Flag("nonewline", "Do not emit newline").Short('n').Bool()
)

func init() {
	metadataShowCommand.Action(showMetadataObject)
}

func showMetadataObject(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)

	b, err := rep.MetadataManager.GetMetadata(*metadataShowID)
	if err != nil {
		return err
	}

	if *metadataShowJSON {
		var buf bytes.Buffer
		json.Indent(&buf, b, "", "  ")
		buf.WriteTo(os.Stdout)
	} else {
		os.Stdout.Write(b)
	}

	if !*metadataShowNoNewLine {
		os.Stdout.WriteString("\n")
	}

	return nil
}
