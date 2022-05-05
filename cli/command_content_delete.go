package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandContentDelete struct {
	ids    []string
	forget bool

	svc appServices
}

func (c *commandContentDelete) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("delete", "Remove content").Alias("remove").Alias("rm")
	cmd.Arg("id", "IDs of content to remove").Required().StringsVar(&c.ids)
	cmd.Flag("forget", "Forget the content instead of marking as deleted - USE WITH EXTREME CAUTION").Hidden().BoolVar(&c.forget)
	cmd.Action(svc.directRepositoryWriteAction(c.run))

	c.svc = svc
}

func (c *commandContentDelete) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	c.svc.advancedCommand(ctx)

	for _, contentID := range toContentIDs(c.ids) {
		if c.forget {
			if err := rep.ContentManager().ForgetContent(ctx, contentID); err != nil {
				return errors.Wrapf(err, "error forgetting content %v", contentID)
			}
		} else {
			if err := rep.ContentManager().DeleteContent(ctx, contentID); err != nil {
				return errors.Wrapf(err, "error deleting content %v", contentID)
			}
		}
	}

	return nil
}
