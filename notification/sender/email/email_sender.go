// Package email provides email notification support.
package email

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/smtp"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
)

// ProviderType defines the type of the email notification provider.
const ProviderType = "email"

const defaultSMTPPort = 587

const smtpSendTimeout = 10 * time.Second

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

	msgPayload = []byte(strings.Join(headers, "\r\n") + "\r\n\r\n" + msg.Body)

	ctx, cancel := context.WithTimeout(ctx, smtpSendTimeout)
	defer cancel()

	return sendMail(ctx, fmt.Sprintf("%v:%d", p.opt.SMTPServer, p.opt.SMTPPort), auth, p.opt.From, strings.Split(p.opt.To, ","), msgPayload)
}

func sendMail(ctx context.Context, addr string, auth smtp.Auth, from string, to []string, msg []byte) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck

	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}

	c, err := smtp.NewClient(conn, host)
	if err != nil {
		return contextError(ctx, err)
	}
	defer c.Close() //nolint:errcheck

	if err := c.Hello("localhost"); err != nil {
		return contextError(ctx, err)
	}
	if ok, _ := c.Extension("STARTTLS"); ok {
		if err := c.StartTLS(&tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}); err != nil {
			return contextError(ctx, err)
		}
	}

	if auth != nil {
		if ok, _ := c.Extension("AUTH"); !ok {
			return errors.New("smtp: server doesn't support AUTH")
		}
		if err := c.Auth(auth); err != nil {
			return contextError(ctx, err)
		}
	}

	if err := c.Mail(from); err != nil {
		return contextError(ctx, err)
	}
	for _, addr := range to {
		if err := c.Rcpt(addr); err != nil {
			return contextError(ctx, err)
		}
	}

	w, err := c.Data()
	if err != nil {
		return contextError(ctx, err)
	}
	if _, err := w.Write(msg); err != nil {
		return contextError(ctx, err)
	}
	if err := w.Close(); err != nil {
		return contextError(ctx, err)
	}

	if err := c.Quit(); err != nil {
		return contextError(ctx, err)
	}

	return nil
}

func contextError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
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
