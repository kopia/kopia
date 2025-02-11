package cli

import (
	"net/http"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/notification/sender/webhook"
)

type commandNotificationConfigureWebhook struct {
	common commonNotificationOptions

	opt webhook.Options
}

func (c *commandNotificationConfigureWebhook) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("webhook", "Webhook notification.")

	c.common.setup(svc, cmd)

	var httpHeaders []string

	cmd.Flag("endpoint", "SMTP server").StringVar(&c.opt.Endpoint)
	cmd.Flag("method", "HTTP Method").EnumVar(&c.opt.Method, http.MethodPost, http.MethodPut)
	cmd.Flag("http-header", "HTTP Header (key:value)").StringsVar(&httpHeaders)
	cmd.Flag("format", "Format of the message").EnumVar(&c.opt.Format, sender.FormatHTML, sender.FormatPlainText)

	act := configureNotificationAction(svc, &c.common, webhook.ProviderType, &c.opt, webhook.MergeOptions)

	cmd.Action(func(ctx *kingpin.ParseContext) error {
		for _, h := range httpHeaders {
			const numParts = 2

			parts := strings.SplitN(h, ":", numParts)
			if len(parts) != numParts {
				return errors.Errorf("invalid --http-header %q, must be key:value", h)
			}
		}

		c.opt.Headers = strings.Join(httpHeaders, "\n")

		return act(ctx)
	})
}
