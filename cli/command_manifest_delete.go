package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandManifestDelete struct {
	manifestRemoveItems []string

	svc appServices
}

func (c *commandManifestDelete) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("delete", "Remove manifest items").Alias("remove").Alias("rm").Hidden()
	cmd.Arg("item", "Items to remove").Required().StringsVar(&c.manifestRemoveItems)
	cmd.Action(svc.repositoryWriterAction(c.run))

	c.svc = svc
}

func (c *commandManifestDelete) run(ctx context.Context, rep repo.RepositoryWriter) error {
	c.svc.dangerousCommand()

	for _, it := range toManifestIDs(c.manifestRemoveItems) {
		if err := rep.DeleteManifest(ctx, it); err != nil {
			return errors.Wrapf(err, "unable to delete manifest %v", it)
		}
	}

	return nil
}
