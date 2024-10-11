// Package pushover provides pushover notification support.
package pushover

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
)

// ProviderType defines the type of the Pushover notification provider.
const ProviderType = "pushover"

// defaultPushoverURL is the default URL for the Pushover API.
const defaultPushoverURL = "https://api.pushover.net/1/messages.json"

type pushoverProvider struct {
	opt Options
}

func (p *pushoverProvider) Send(ctx context.Context, msg *sender.Message) error {
	payload := map[string]string{
		"token":   p.opt.AppToken,
		"user":    p.opt.UserKey,
		"message": msg.Subject + "\n\n" + msg.Body,
	}

	if p.Format() == "html" {
		payload["html"] = "1"
	}

	targetURL := defaultPushoverURL
	if p.opt.Endpoint != "" {
		targetURL = p.opt.Endpoint
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return errors.Wrap(err, "error preparing pushover notification")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return errors.Wrap(err, "error preparing pushover notification")
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "error sending pushover notification")
	}

	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("error sending pushover notification: %v", resp.Status)
	}

	return nil
}

func (p *pushoverProvider) Summary() string {
	return fmt.Sprintf("Pushover user %q app %q format %q", p.opt.UserKey, p.opt.AppToken, p.Format())
}

func (p *pushoverProvider) Format() string {
	return p.opt.Format
}

func init() {
	sender.Register(ProviderType, func(ctx context.Context, options *Options) (sender.Provider, error) {
		if err := options.ApplyDefaultsAndValidate(ctx); err != nil {
			return nil, errors.Wrap(err, "invalid notification configuration")
		}

		return &pushoverProvider{
			opt: *options,
		}, nil
	})
}
