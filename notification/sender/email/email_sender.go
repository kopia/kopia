// Package email provides email notification support.
package email

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/smtp"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
)

// ProviderType defines the type of the email notification provider.
const ProviderType = "email"

const defaultSMTPPort = 587

// smtpSendTimeout bounds the whole SMTP exchange, from dialing through QUIT.
// It is a variable so that tests can shorten it.
var smtpSendTimeout = 10 * time.Second

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

	return sendMail(ctx, fmt.Sprintf("%v:%d", p.opt.SMTPServer, p.opt.SMTPPort), auth, p.opt.From, strings.Split(p.opt.To, ","), msgPayload)
}

// sendMail delivers msg over SMTP. The exchange is bounded by smtpSendTimeout, or by
// any earlier deadline or cancellation already carried by ctx.
func sendMail(ctx context.Context, addr string, auth smtp.Auth, from string, to []string, msg []byte) error {
	ctx, cancel := context.WithTimeout(ctx, smtpSendTimeout)
	defer cancel()

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return errors.Wrap(err, "invalid SMTP server address")
	}

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return contextError(ctx, errors.Wrap(err, "unable to connect to SMTP server"))
	}

	defer conn.Close() //nolint:errcheck

	// A connection deadline is an absolute instant and never observes ctx.Done(), so
	// closing the connection is what unblocks an in-flight read when the caller cancels.
	defer context.AfterFunc(ctx, func() {
		conn.Close() //nolint:errcheck
	})()

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return errors.Wrap(err, "unable to set connection deadline")
		}
	}

	return sendMailOnConn(ctx, conn, host, auth, from, to, msg)
}

func sendMailOnConn(ctx context.Context, conn net.Conn, host string, auth smtp.Auth, from string, to []string, msg []byte) error {
	c, err := smtp.NewClient(conn, host)
	if err != nil {
		return contextError(ctx, errors.Wrap(err, "unable to start SMTP session"))
	}

	defer c.Close() //nolint:errcheck

	if err := c.Hello("localhost"); err != nil {
		return contextError(ctx, errors.Wrap(err, "SMTP greeting failed"))
	}

	if ok, _ := c.Extension("STARTTLS"); ok {
		if err := c.StartTLS(&tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}); err != nil {
			return contextError(ctx, errors.Wrap(err, "STARTTLS failed"))
		}
	}

	if auth != nil {
		if ok, _ := c.Extension("AUTH"); !ok {
			return errors.New("smtp: server doesn't support AUTH")
		}

		if err := c.Auth(auth); err != nil {
			return contextError(ctx, errors.Wrap(err, "SMTP authentication failed"))
		}
	}

	return writeMessage(ctx, c, from, to, msg)
}

func writeMessage(ctx context.Context, c *smtp.Client, from string, to []string, msg []byte) error {
	if err := c.Mail(from); err != nil {
		return contextError(ctx, errors.Wrap(err, "SMTP MAIL command failed"))
	}

	for _, rcpt := range to {
		if err := c.Rcpt(rcpt); err != nil {
			return contextError(ctx, errors.Wrap(err, "SMTP RCPT command failed"))
		}
	}

	w, err := c.Data()
	if err != nil {
		return contextError(ctx, errors.Wrap(err, "SMTP DATA command failed"))
	}

	if _, err := w.Write(msg); err != nil {
		return contextError(ctx, errors.Wrap(err, "unable to write message body"))
	}

	if err := w.Close(); err != nil {
		return contextError(ctx, errors.Wrap(err, "unable to finish message body"))
	}

	if err := c.Quit(); err != nil {
		return contextError(ctx, errors.Wrap(err, "SMTP QUIT failed"))
	}

	return nil
}

// contextError reports the context's own error when the exchange was stopped by the
// deadline or by cancellation, keeping err in the chain for diagnostics. The connection
// deadline is armed from the context deadline, so os.ErrDeadlineExceeded describes the
// same expiry even when the context's own timer has not fired yet.
func contextError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("%w: %w", ctxErr, err)
	}

	if errors.Is(err, os.ErrDeadlineExceeded) {
		return fmt.Errorf("%w: %w", context.DeadlineExceeded, err)
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
