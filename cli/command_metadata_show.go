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
	metadataShowRaw       = metadataShowCommand.Flag("r", "Don't pretty-print JSON").Short('r').Bool()
	metadataShowNoNewLine = metadataShowCommand.Flag("nonewline", "Do not emit newline").Short('n').Bool()
)

func init() {
	metadataShowCommand.Action(showMetadataObject)
}

func showMetadataObject(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)

	b, err := rep.Metadata.GetMetadata(*metadataShowID)
	if err != nil {
		return err
	}

	if !*metadataShowRaw && len(b) > 0 && b[0] == '{' {
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
