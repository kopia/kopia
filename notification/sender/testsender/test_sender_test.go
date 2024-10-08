package testsender_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/notification/sender/testsender"
)

func TestProvider(t *testing.T) {
	ctx := testlogging.Context(t)

	ctx = testsender.CaptureMessages(ctx)

	p, err := sender.GetSender(ctx, "my-profile", "testsender", &testsender.Options{
		Format: sender.FormatPlainText,
	})
	require.NoError(t, err)

	require.Equal(t, "Test sender", p.Summary())
	m1 := &sender.Message{
		Subject: "test subject 1",
	}
	m2 := &sender.Message{
		Subject: "test subject 2",
	}
	m3 := &sender.Message{
		Subject: "test subject 3",
	}
	p.Send(ctx, m1)
	p.Send(ctx, m2)
	p.Send(ctx, m3)
	mic := testsender.MessagesInContext(ctx)
	require.ElementsMatch(t, mic, []*sender.Message{m1, m2, m3})
}

func TestProvider_NotConfigured(t *testing.T) {
	ctx := testlogging.Context(t)

	// do not call 'ctx = testsender.CaptureMessages(ctx)'
	p, err := sender.GetSender(ctx, "my-profile", "testsender", &testsender.Options{
		Format: "txt",
	})
	require.NoError(t, err)

	require.Equal(t, "Test sender", p.Summary())
	m1 := &sender.Message{
		Subject: "test subject 1",
	}
	p.Send(ctx, m1)

	// nothing captured
	require.Empty(t, testsender.MessagesInContext(ctx))
}
