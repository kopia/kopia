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

	raw bool
}

func (c *commandNotificationProfileList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List notification profiles").Alias("ls")

	c.out.setup(svc)
	c.jo.setup(svc, cmd)

	cmd.Flag("raw", "Raw output").BoolVar(&c.raw)

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
		summ := getProfileSummary(ctx, pc)

		if c.jo.jsonOutput {
			if c.raw {
				jl.emit(pc)
			} else {
				jl.emit(summ)
			}
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

func getProfileSummary(ctx context.Context, pc notifyprofile.Config) notifyprofile.Summary {
	var summ notifyprofile.Summary

	summ.ProfileName = pc.ProfileName
	summ.Type = string(pc.MethodConfig.Type)
	summ.MinSeverity = int32(pc.MinSeverity)

	// Provider returns a new instance of the notification provider.
	if prov, err := sender.GetSender(ctx, pc.ProfileName, pc.MethodConfig.Type, pc.MethodConfig.Config); err == nil {
		summ.Summary = prov.Summary()
	} else {
		summ.Summary = fmt.Sprintf("%v - invalid", pc.MethodConfig.Type)
	}

	return summ
}
