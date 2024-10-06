package cli

import (
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/notification/sender/email"
)

type commandNotificationConfigureEmail struct {
	common commonNotificationOptions

	opt email.Options
}

func (c *commandNotificationConfigureEmail) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("email", "E-mail notification.")

	c.common.setup(svc, cmd)

	cmd.Flag("smtp-server", "SMTP server").StringVar(&c.opt.SMTPServer)
	cmd.Flag("smtp-port", "SMTP port").IntVar(&c.opt.SMTPPort)
	cmd.Flag("smtp-identity", "SMTP identity").StringVar(&c.opt.SMTPIdentity)
	cmd.Flag("smtp-username", "SMTP username").StringVar(&c.opt.SMTPUsername)
	cmd.Flag("smtp-password", "SMTP password").StringVar(&c.opt.SMTPPassword)
	cmd.Flag("mail-from", "From address").StringVar(&c.opt.From)
	cmd.Flag("mail-to", "To address").StringVar(&c.opt.To)
	cmd.Flag("mail-cc", "CC address").StringVar(&c.opt.CC)

	cmd.Flag("format", "Format of the message").EnumVar(&c.opt.Format, sender.FormatHTML, sender.FormatPlainText)

	cmd.Action(configureNotificationAction(svc, &c.common, email.ProviderType, &c.opt, email.MergeOptions))
}
