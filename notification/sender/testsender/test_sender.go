// Package testsender provides notification sender testing support.
package testsender

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
)

// ProviderType defines the type of the test notification provider.
const ProviderType = "testsender"

type capturedMessagesContextKeyType string

// capturedMessagesContextKey is a context key for captured messages.
const capturedMessagesContextKey capturedMessagesContextKeyType = "capturedMessages"

type capturedMessages struct {
	messages []*sender.Message
	handle   func(*sender.Message) error
}

// CaptureMessages captures messages sent in the provider context and returns a new context.
// Captured messages can be retrieved using MessagesInContext.
func CaptureMessages(ctx context.Context) context.Context {
	cm := &capturedMessages{}

	cm.handle = func(msg *sender.Message) error {
		cm.messages = append(cm.messages, msg)
		return nil
	}

	return context.WithValue(ctx, capturedMessagesContextKey, cm)
}

// CaptureMessagesWithHandler captures messages sent in the provider context and returns a new context.
// Captured messages can be retrieved using MessagesInContext.
func CaptureMessagesWithHandler(ctx context.Context, handler func(msg *sender.Message) error) context.Context {
	cm := &capturedMessages{
		handle: handler,
	}

	return context.WithValue(ctx, capturedMessagesContextKey, cm)
}

// MessagesInContext retrieves messages sent in the provider context.
func MessagesInContext(ctx context.Context) []*sender.Message {
	if v, ok := ctx.Value(capturedMessagesContextKey).(*capturedMessages); ok {
		return v.messages
	}

	return nil
}

type testSenderProvider struct {
	mu sync.Mutex

	opt Options
}

func (p *testSenderProvider) Send(ctx context.Context, msg *sender.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	cm, ok := ctx.Value(capturedMessagesContextKey).(*capturedMessages)
	if !ok {
		return errors.Errorf("test sender not configured")
	}

	return cm.handle(msg)
}

func (p *testSenderProvider) Summary() string {
	return "Test sender"
}

func (p *testSenderProvider) Format() string {
	return p.opt.Format
}

func init() {
	sender.Register(ProviderType, func(ctx context.Context, options *Options) (sender.Provider, error) {
		if err := options.ApplyDefaultsAndValidate(ctx); err != nil {
			return nil, errors.Wrap(err, "invalid notification configuration")
		}

		return &testSenderProvider{
			opt: *options,
		}, nil
	})
}
