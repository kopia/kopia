package cli

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/notification/notifyprofile"
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/repo"
)

type commandNotificationProfileList struct {
	out textOutput
	jo  jsonOutput
}

func (c *commandNotificationProfileList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List notification profiles").Alias("ls")

	c.out.setup(svc)
	c.jo.setup(svc, cmd)

	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandNotificationProfileList) run(ctx context.Context, rep repo.Repository) error {
	var jl jsonList

	if c.jo.jsonOutput {
		jl.begin(&c.jo)
		defer jl.end()
	}

	profileConfigs, err := notifyprofile.ListProfiles(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "unable to list notification profiles")
	}

	for i, pc := range profileConfigs {
		var summ notifyprofile.Summary

		summ.ProfileName = pc.Profile
		summ.Type = string(pc.MethodConfig.Type)
		summ.MinSeverity = int32(pc.MinSeverity)

		// Provider returns a new instance of the notification provider.
		if prov, err := sender.GetSender(ctx, pc.Profile, pc.MethodConfig.Type, pc.MethodConfig.Config); err == nil {
			summ.Summary = prov.Summary()
		} else {
			summ.Summary = fmt.Sprintf("%v - invalid", pc.MethodConfig.Type)
		}

		if c.jo.jsonOutput {
			jl.emit(summ)
		} else {
			if i > 0 {
				c.out.printStdout("\n")
			}

			c.out.printStdout("Profile %q Type %q Minimum Severity: %v\n  %v\n",
				summ.ProfileName,
				pc.MethodConfig.Type,
				notification.SeverityToString[pc.MinSeverity],
				summ.Summary)
		}
	}

	return nil
}
