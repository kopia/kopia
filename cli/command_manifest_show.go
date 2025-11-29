package cli

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
)

type commandManifestShow struct {
	manifestShowItems []string

	out textOutput
}

func (c *commandManifestShow) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("show", "Show manifest items")
	cmd.Arg("item", "List of items").Required().StringsVar(&c.manifestShowItems)
	cmd.Action(svc.repositoryReaderAction(c.showManifestItems))
	c.out.setup(svc)
}

func toManifestIDs(s []string) []manifest.ID {
	result := make([]manifest.ID, 0, len(s))

	for _, it := range s {
		result = append(result, manifest.ID(it))
	}

	return result
}

func (c *commandManifestShow) showManifestItems(ctx context.Context, rep repo.Repository) error {
	for _, it := range toManifestIDs(c.manifestShowItems) {
		var b json.RawMessage

		md, err := rep.GetManifest(ctx, it, &b)
		if err != nil {
			return errors.Wrapf(err, "error getting metadata for %q", it)
		}

		c.out.printStdout("// id: %v\n", it)
		c.out.printStdout("// length: %v\n", md.Length)
		c.out.printStdout("// modified: %v\n", formatTimestamp(md.ModTime))

		for k, v := range md.Labels {
			c.out.printStdout("// label %v:%v\n", k, v)
		}

		if showerr := showContentWithFlags(c.out.stdout(), bytes.NewReader(b), false, true); showerr != nil {
			return showerr
		}
	}

	return nil
}
