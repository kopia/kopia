// Package nats provides NATS notification support.
package nats

import (
	"context"
	"fmt"

	natslib "github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/notification/sender"
)

// ProviderType defines the type of the NATS notification provider.
const ProviderType = "nats"

const defaultConnectionName = "kopia"

type natsProvider struct {
	opt Options
}

func (p *natsProvider) connectOptions(ctx context.Context) ([]natslib.Option, error) {
	opts := []natslib.Option{
		natslib.Name(p.opt.ConnectionName),
		natslib.NoReconnect(),
	}

	if dl, ok := ctx.Deadline(); ok {
		remaining := dl.Sub(clock.Now())
		if remaining <= 0 {
			return nil, errors.Wrap(context.DeadlineExceeded, "NATS server connect deadline exceeded")
		}

		opts = append(opts, natslib.Timeout(remaining))
	}

	switch {
	case p.opt.CredentialsFile != "":
		opts = append(opts, natslib.UserCredentials(p.opt.CredentialsFile))
	case p.opt.NKeySeedFile != "":
		o, err := natslib.NkeyOptionFromSeed(p.opt.NKeySeedFile)
		if err != nil {
			return nil, errors.Wrap(err, "error loading NKey seed file")
		}

		opts = append(opts, o)
	case p.opt.Token != "":
		opts = append(opts, natslib.Token(p.opt.Token))
	case p.opt.Username != "":
		opts = append(opts, natslib.UserInfo(p.opt.Username, p.opt.Password))
	}

	if p.opt.TLSCertificateFile != "" || p.opt.TLSKeyFile != "" {
		opts = append(opts, natslib.ClientCert(p.opt.TLSCertificateFile, p.opt.TLSKeyFile))
	}

	if len(p.opt.TLSCertificateAuthorityFile) != 0 {
		opts = append(opts, natslib.RootCAs(p.opt.TLSCertificateAuthorityFile...))
	}

	if p.opt.TLSFirst {
		opts = append(opts, natslib.TLSHandshakeFirst())
	}

	if p.opt.TLSInsecureSkipVerify {
		opts = append(opts, natslib.Secure())
	}

	return opts, nil
}

// flushWithContextOptionalDeadline flushes the NATS connection, honoring ctx's
// deadline if it has one. FlushWithContext requires a context with a deadline,
// so when ctx has none we fall back to Flush(), which applies the NATS client's
// own default timeout instead.
func flushWithContextOptionalDeadline(ctx context.Context, nc *natslib.Conn) error {
	if _, ok := ctx.Deadline(); ok {
		return errors.Wrap(nc.FlushWithContext(ctx), "error flushing NATS connection")
	}

	return errors.Wrap(nc.Flush(), "error flushing NATS connection")
}

func (p *natsProvider) Send(ctx context.Context, msg *sender.Message) error {
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "context already done before connecting to NATS server")
	}

	opts, err := p.connectOptions(ctx)
	if err != nil {
		return err
	}

	// natslib.Connect does not have a variant which accepts a context. A
	// timeout is therefore configured in the connection options, and we check
	// ctx.Err() after Connect returns.
	nc, err := natslib.Connect(p.opt.ServerURL, opts...)
	if err != nil {
		return errors.Wrap(err, "error connecting to NATS server")
	}

	defer nc.Close()

	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "context done after connecting to NATS server")
	}

	m := &natslib.Msg{
		Subject: p.opt.Subject,
		Data:    []byte(msg.Body),
		Header:  natslib.Header{},
	}

	m.Header.Set("Subject", msg.Subject)

	for k, v := range msg.Headers {
		m.Header.Set(k, v)
	}

	if err := nc.PublishMsg(m); err != nil {
		return errors.Wrap(err, "error publishing NATS notification")
	}

	if err := flushWithContextOptionalDeadline(ctx, nc); err != nil {
		return err
	}

	return nil
}

func (p *natsProvider) Summary() string {
	return fmt.Sprintf("NATS %v subject %q format %q", p.opt.ServerURL, p.opt.Subject, p.Format())
}

func (p *natsProvider) Format() string {
	return p.opt.Format
}

func init() {
	sender.Register(ProviderType, func(ctx context.Context, options *Options) (sender.Provider, error) {
		if err := options.ApplyDefaultsAndValidate(ctx); err != nil {
			return nil, errors.Wrap(err, "invalid notification configuration")
		}

		return &natsProvider{
			opt: *options,
		}, nil
	})
}
