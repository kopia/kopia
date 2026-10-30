package cli

import (
	"context"

	"github.com/kopia/kopia/notification/notifyprofile"
	"github.com/kopia/kopia/repo"
)

type commandNotificationProfileDelete struct {
	notificationProfileFlag
}

func (c *commandNotificationProfileDelete) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("delete", "Delete notification profile").Alias("rm")

	c.notificationProfileFlag.setup(svc, cmd)

	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandNotificationProfileDelete) run(ctx context.Context, rep repo.RepositoryWriter) error {
	//nolint:wrapcheck
	return notifyprofile.DeleteProfile(ctx, rep, c.profileName)
}
