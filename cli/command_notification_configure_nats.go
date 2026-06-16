package cli

import (
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/notification/sender/nats"
)

type commandNotificationConfigureNats struct {
	common commonNotificationOptions

	opt nats.Options
}

func (c *commandNotificationConfigureNats) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("nats", "NATS notification.")

	c.common.setup(svc, cmd)

	cmd.Flag("server-url", "NATS server URL (e.g. nats://localhost:4222)").StringVar(&c.opt.ServerURL)
	cmd.Flag("subject", "NATS subject (topic) to publish to").StringVar(&c.opt.Subject)
	cmd.Flag("username", "NATS username").StringVar(&c.opt.Username)
	// NB: cannot be named "--password", it collides with the global repository password flag.
	cmd.Flag("nats-password", "NATS password").StringVar(&c.opt.Password)
	cmd.Flag("token", "NATS authentication token").StringVar(&c.opt.Token)
	cmd.Flag("credentials-file", "Path to NATS .creds (JWT) file").StringVar(&c.opt.CredentialsFile)
	cmd.Flag("nkey-seed-file", "Path to NATS NKey seed file").StringVar(&c.opt.NKeySeedFile)
	cmd.Flag("insecure-skip-verify", "Skip TLS certificate verification").BoolVar(&c.opt.InsecureSkipVerify)

	cmd.Flag("format", "Format of the message").EnumVar(&c.opt.Format, sender.FormatHTML, sender.FormatPlainText, sender.FormatJSON)

	cmd.Action(configureNotificationAction(svc, &c.common, nats.ProviderType, &c.opt, nats.MergeOptions))
}
