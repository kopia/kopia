package cli

import (
	"bytes"
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

type commandContentShow struct {
	ids        []string
	indentJSON bool
	decompress bool

	out textOutput
}

func (c *commandContentShow) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("show", "Show contents by ID.").Alias("cat")

	cmd.Arg("id", "IDs of contents to show").Required().StringsVar(&c.ids)
	cmd.Flag("json", "Pretty-print JSON content").Short('j').BoolVar(&c.indentJSON)
	cmd.Flag("unzip", "Transparently decompress the content").Short('z').BoolVar(&c.decompress)
	cmd.Action(svc.directRepositoryReadAction(c.run))

	c.out.setup(svc)
}

func (c *commandContentShow) run(ctx context.Context, rep repo.DirectRepository) error {
	contentIDs, err := toContentIDs(c.ids)
	if err != nil {
		return err
	}

	for _, contentID := range contentIDs {
		if err := c.contentShow(ctx, rep, contentID); err != nil {
			return err
		}
	}

	return nil
}

func (c *commandContentShow) contentShow(ctx context.Context, r repo.DirectRepository, contentID content.ID) error {
	data, err := r.ContentReader().GetContent(ctx, contentID)
	if err != nil {
		return errors.Wrapf(err, "error getting content %v", contentID)
	}

	return showContentWithFlags(c.out.stdout(), bytes.NewReader(data), c.decompress, c.indentJSON)
}
