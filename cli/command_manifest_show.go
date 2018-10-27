package cli

import (
	"bytes"
	"context"
	"fmt"

	"github.com/kopia/repo"
)

var (
	manifestShowCommand = manifestCommands.Command("show", "Show manifest items")
	manifestShowItems   = manifestShowCommand.Arg("item", "List of items").Required().Strings()
)

func init() {
	manifestShowCommand.Action(repositoryAction(showManifestItems))
}

func showManifestItems(ctx context.Context, rep *repo.Repository) error {
	for _, it := range *manifestShowItems {
		md, err := rep.Manifests.GetMetadata(ctx, it)
		if err != nil {
			return fmt.Errorf("error getting metadata for %q: %v", it, err)
		}

		b, err := rep.Manifests.GetRaw(ctx, it)
		if err != nil {
			return fmt.Errorf("error showing %q: %v", it, err)
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
