// Package email provides email notification support.
package email

import (
	"context"
	"fmt"
	"net/smtp"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
)

// ProviderType defines the type of the email notification provider.
const ProviderType = "email"

const defaultSMTPPort = 587

type emailProvider struct {
	opt Options
}

func (p *emailProvider) Send(ctx context.Context, msg *sender.Message) error {
	var auth smtp.Auth

	if p.opt.SMTPUsername != "" {
		auth = smtp.PlainAuth(p.opt.SMTPIdentity, p.opt.SMTPUsername, p.opt.SMTPPassword, p.opt.SMTPServer)
	}

	var msgPayload []byte

	headers := []string{
		"Subject: " + msg.Subject,
		"From: " + p.opt.From,
		"To: " + p.opt.To,
	}

	if p.Format() == sender.FormatHTML {
		headers = append(headers,
			"MIME-version: 1.0;",
			"Content-Type: text/html; charset=\"UTF-8\";",
		)
	}

	for k, v := range msg.Headers {
		headers = append(headers, fmt.Sprintf("%v: %v", k, v))
	}

	msgPayload = []byte(strings.Join(headers, "\r\n") + "\r\n" + msg.Body)

	//nolint:wrapcheck
	return smtp.SendMail(
		fmt.Sprintf("%v:%d", p.opt.SMTPServer, p.opt.SMTPPort),
		auth,
		p.opt.From,
		strings.Split(p.opt.To, ","),
		msgPayload)
}

func (p *emailProvider) Summary() string {
	return fmt.Sprintf("SMTP server: %q, Mail from: %q Mail to: %q Format: %q", p.opt.SMTPServer, p.opt.From, p.opt.To, p.Format())
}

func (p *emailProvider) Format() string {
	return p.opt.Format
}

func init() {
	sender.Register(ProviderType, func(ctx context.Context, options *Options) (sender.Provider, error) {
		if err := options.ApplyDefaultsAndValidate(ctx); err != nil {
			return nil, errors.Wrap(err, "invalid notification configuration")
		}

		return &emailProvider{
			opt: *options,
		}, nil
	})
}
