package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandContentDelete struct {
	ids []string

	svc appServices
}

func (c *commandContentDelete) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("delete", "Remove content").Alias("remove").Alias("rm")
	cmd.Arg("id", "IDs of content to remove").Required().StringsVar(&c.ids)
	cmd.Action(svc.directRepositoryWriteAction(c.run))

	c.svc = svc
}

func (c *commandContentDelete) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	c.svc.advancedCommand(ctx)

	contentIDs, err := toContentIDs(c.ids)
	if err != nil {
		return err
	}

	for _, contentID := range contentIDs {
		if err := rep.ContentManager().DeleteContent(ctx, contentID); err != nil {
			return errors.Wrapf(err, "error deleting content %v", contentID)
		}
	}

	return nil
}
