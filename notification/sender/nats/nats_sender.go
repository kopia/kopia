// Package nats provides NATS notification support.
package nats

import (
	"context"
	"fmt"
	"time"

	natslib "github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
)

// ProviderType defines the type of the NATS notification provider.
const ProviderType = "nats"

const (
	connectTimeout = 10 * time.Second
	flushTimeout   = 10 * time.Second
)

type natsProvider struct {
	opt Options
}

func (p *natsProvider) connectOptions() ([]natslib.Option, error) {
	opts := []natslib.Option{
		natslib.Name("kopia"),
		natslib.Timeout(connectTimeout),
		natslib.NoReconnect(),
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

	if p.opt.InsecureSkipVerify {
		opts = append(opts, natslib.Secure())
	}

	return opts, nil
}

func (p *natsProvider) Send(_ context.Context, msg *sender.Message) error {
	opts, err := p.connectOptions()
	if err != nil {
		return err
	}

	nc, err := natslib.Connect(p.opt.ServerURL, opts...)
	if err != nil {
		return errors.Wrap(err, "error connecting to NATS server")
	}

	defer nc.Close()

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

	if err := nc.FlushTimeout(flushTimeout); err != nil {
		return errors.Wrap(err, "error flushing NATS connection")
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
