package cli

import (
	"context"
	"strings"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/notification/notifyprofile"
	"github.com/kopia/kopia/repo"
)

type commandNotificationProfile struct {
	config commandNotificationProfileConfigure
	list   commandNotificationProfileList
	delete commandNotificationProfileDelete
	test   commandNotificationProfileTest
	show   commandNotificationProfileShow
}

func (c *commandNotificationProfile) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("profile", "Manage notification profiles")
	c.config.setup(svc, cmd)
	c.delete.setup(svc, cmd)
	c.test.setup(svc, cmd)
	c.list.setup(svc, cmd)
	c.show.setup(svc, cmd)
}

type notificationProfileFlag struct {
	profileName string
}

func (c *notificationProfileFlag) setup(svc appServices, cmd *kingpin.CmdClause) {
	cmd.Flag("profile-name", "Profile name").Required().HintAction(svc.repositoryHintAction(c.listNotificationProfiles)).StringVar(&c.profileName)
}

func (c *notificationProfileFlag) listNotificationProfiles(ctx context.Context, rep repo.Repository) []string {
	profiles, err := notifyprofile.ListProfiles(ctx, rep)
	if err != nil {
		return nil
	}

	var hints []string

	for _, ti := range profiles {
		if strings.HasPrefix(ti.ProfileName, c.profileName) {
			hints = append(hints, ti.ProfileName)
		}
	}

	return hints
}
