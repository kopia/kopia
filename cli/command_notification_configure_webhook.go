package cli

import (
	"net/http"

	"github.com/kopia/kopia/notification/sender/webhook"
)

type commandNotificationConfigureWebhook struct {
	common commonNotificationOptions

	opt webhook.Options
}

func (c *commandNotificationConfigureWebhook) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("webhook", "Webhook notification.")

	c.common.setup(svc, cmd)

	cmd.Flag("endpoint", "SMTP server").StringVar(&c.opt.Endpoint)
	cmd.Flag("method", "HTTP Method").EnumVar(&c.opt.Method, http.MethodPost, http.MethodPut)

	cmd.Action(configureNotificationAction(svc, &c.common, webhook.ProviderType, &c.opt, webhook.MergeOptions))
}
