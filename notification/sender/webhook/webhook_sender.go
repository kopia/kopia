// Package webhook provides webhook notification support.
package webhook

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
)

// ProviderType defines the type of the Webhook notification provider.
const ProviderType = "webhook"

type webhookProvider struct {
	opt Options
}

func (p *webhookProvider) Send(ctx context.Context, msg *sender.Message) error {
	targetURL := p.opt.Endpoint
	method := p.opt.Method

	body := bytes.NewReader([]byte(msg.Body))

	req, err := http.NewRequestWithContext(ctx, method, targetURL, body)
	if err != nil {
		return errors.Wrap(err, "error preparing notification")
	}

	req.Header.Set("Subject", msg.Subject)

	// copy headers from message
	for k, v := range msg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "error sending webhook notification")
	}

	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("error sending webhook notification: %v", resp.Status)
	}

	return nil
}

func (p *webhookProvider) Summary() string {
	return fmt.Sprintf("Webhook %v %v", p.opt.Method, p.opt.Endpoint)
}

func (p *webhookProvider) Format() string {
	return p.opt.Format
}

func init() {
	sender.Register(ProviderType, func(ctx context.Context, options *Options) (sender.Provider, error) {
		if err := options.ApplyDefaultsAndValidate(ctx); err != nil {
			return nil, errors.Wrap(err, "invalid notification configuration")
		}

		return &webhookProvider{
			opt: *options,
		}, nil
	})
}
