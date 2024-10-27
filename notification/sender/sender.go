// Package sender provides a common interface for sending notifications.
package sender

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("notification/sender")

// Provider is an interface implemented by all notification providers.
type Provider interface {
	Send(ctx context.Context, msg *Message) error

	// Format returns the format of the message body that the provider supports, either "html" or "md", some providers will support both.
	Format() string

	// Summary returns a human-readable summary of the provider configuration.
	Summary() string
}

// Sender is an interface implemented by all notification senders that also provide a profile name.
type Sender interface {
	Provider

	ProfileName() string
}

// Factory is a function that creates a new instance of a notification sender with a
// given context and options.
type Factory[T any] func(ctx context.Context, options T) (Provider, error)

//nolint:gochecknoglobals
var (
	allSenders     = map[Method]Factory[any]{}
	defaultOptions = map[Method]any{}
)

type senderWrapper struct {
	Provider
	profileName string
}

func (s senderWrapper) ProfileName() string {
	return s.profileName
}

// GetSender returns a new instance of a sender with a given name and options.
func GetSender(ctx context.Context, profile string, method Method, jsonOptions any) (Sender, error) {
	factory := allSenders[method]
	if factory == nil {
		return nil, errors.Errorf("unknown sender: %v", method)
	}

	sp, err := factory(ctx, jsonOptions)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create sender")
	}

	return senderWrapper{sp, profile}, nil
}

// Register registers a new provider with a given name and factory function.
func Register[T any](method Method, p Factory[*T]) {
	var defT T

	defaultOptions[method] = defT

	allSenders[method] = func(ctx context.Context, jsonOptions any) (Provider, error) {
		typedOptions := defT

		v, err := json.Marshal(jsonOptions)
		if err != nil {
			return nil, errors.Wrap(err, "unable to marshal options")
		}

		if err := json.Unmarshal(v, &typedOptions); err != nil {
			return nil, errors.Wrap(err, "unable to unmarshal options")
		}

		return p(ctx, &typedOptions)
	}
}
