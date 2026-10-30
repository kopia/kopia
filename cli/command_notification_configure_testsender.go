package cli

import (
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/notification/sender/testsender"
)

type commandNotificationConfigureTestSender struct {
	common commonNotificationOptions

	opt testsender.Options
}

func (c *commandNotificationConfigureTestSender) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("testsender", "Testing notification.")

	c.common.setup(svc, cmd)
	cmd.Flag("format", "Format of the message").EnumVar(&c.opt.Format, sender.FormatHTML, sender.FormatPlainText)

	cmd.Action(configureNotificationAction(svc, &c.common, testsender.ProviderType, &c.opt, testsender.MergeOptions))
}
