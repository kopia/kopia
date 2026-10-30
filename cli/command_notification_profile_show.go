package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/notification/notifyprofile"
	"github.com/kopia/kopia/repo"
)

type commandNotificationProfileShow struct {
	out textOutput
	jo  jsonOutput
	notificationProfileFlag

	raw bool
}

func (c *commandNotificationProfileShow) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("show", "Show notification profile")

	c.out.setup(svc)
	c.jo.setup(svc, cmd)
	c.notificationProfileFlag.setup(svc, cmd)

	cmd.Flag("raw", "Raw output").BoolVar(&c.raw)

	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandNotificationProfileShow) run(ctx context.Context, rep repo.Repository) error {
	pc, err := notifyprofile.GetProfile(ctx, rep, c.profileName)
	if err != nil {
		return errors.Wrap(err, "unable to list notification profiles")
	}

	summ := getProfileSummary(ctx, pc)

	if !c.jo.jsonOutput {
		c.out.printStdout("Profile %q Type %q Minimum Severity: %v\n%v\n",
			summ.ProfileName,
			pc.MethodConfig.Type,
			notification.SeverityToString[pc.MinSeverity],
			summ.Summary)

		return nil
	}

	if c.raw {
		c.out.printStdout("%s\n", c.jo.jsonBytes(pc))
	} else {
		c.out.printStdout("%s\n", c.jo.jsonBytes(summ))
	}

	return nil
}
