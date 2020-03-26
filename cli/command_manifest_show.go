package cli

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
)

var (
	manifestShowCommand = manifestCommands.Command("show", "Show manifest items")
	manifestShowItems   = manifestShowCommand.Arg("item", "List of items").Required().Strings()
)

func init() {
	manifestShowCommand.Action(repositoryAction(showManifestItems))
}

func toManifestIDs(s []string) []manifest.ID {
	var result []manifest.ID

	for _, it := range s {
		result = append(result, manifest.ID(it))
	}

	return result
}

func showManifestItems(ctx context.Context, rep repo.Repository) error {
	for _, it := range toManifestIDs(*manifestShowItems) {
		var b json.RawMessage

		md, err := rep.GetManifest(ctx, it, &b)
		if err != nil {
			return errors.Wrapf(err, "error getting metadata for %q", it)
		}

		printStderr("// id: %v\n", it)
		printStderr("// length: %v\n", md.Length)
		printStderr("// modified: %v\n", formatTimestamp(md.ModTime))

		for k, v := range md.Labels {
			printStderr("// label %v:%v\n", k, v)
		}

		if showerr := showContentWithFlags(bytes.NewReader(b), false, true); showerr != nil {
			return showerr
		}
	}

	return nil
}
