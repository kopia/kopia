package cli

import (
	"github.com/kopia/kopia/notification/sender/pushover"
)

type commandNotificationConfigurePushover struct {
	common commonNotificationOptions

	opt pushover.Options
}

func (c *commandNotificationConfigurePushover) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("pushover", "Pushover notification.")

	c.common.setup(svc, cmd)

	cmd.Flag("app-token", "Pushover App Token").StringVar(&c.opt.AppToken)
	cmd.Flag("user-key", "Pushover User Key").StringVar(&c.opt.UserKey)

	cmd.Action(configureNotificationAction(svc, &c.common, pushover.ProviderType, &c.opt, pushover.MergeOptions))
}
