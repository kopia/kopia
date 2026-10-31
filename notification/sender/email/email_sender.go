// Package email provides email notification support.
package email

import (
	"context"
	"fmt"
	"net/mail"
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

func (p *emailProvider) Send(_ context.Context, msg *sender.Message) error {
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

	msgPayload = []byte(strings.Join(headers, "\r\n") + "\r\n\r\n" + msg.Body)

	//nolint:wrapcheck
	return smtp.SendMail(
		fmt.Sprintf("%v:%d", p.opt.SMTPServer, p.opt.SMTPPort),
		auth,
		envelopeAddress(p.opt.From),
		envelopeAddresses(p.opt.To),
		msgPayload,
	)
}

// envelopeAddress returns the bare address to use in the SMTP envelope (MAIL FROM, RCPT TO)
// for a value that may also contain a display name, such as "Name <user@example.com>".
// If the value can't be parsed it is returned unchanged.
func envelopeAddress(s string) string {
	a, err := mail.ParseAddress(s)
	if err != nil {
		return s
	}

	return a.Address
}

// envelopeAddresses returns the bare addresses to use in the SMTP envelope for a comma-separated
// list of addresses that may contain display names.
// If the list can't be parsed it is split on commas without any other processing.
func envelopeAddresses(s string) []string {
	list, err := mail.ParseAddressList(s)
	if err != nil {
		return strings.Split(s, ",")
	}

	result := make([]string, 0, len(list))
	for _, a := range list {
		result = append(result, a.Address)
	}

	return result
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
