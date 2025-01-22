package jsonsender_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/notification/sender/jsonsender"
)

func TestJSONSender(t *testing.T) {
	ctx := testlogging.Context(t)

	var buf bytes.Buffer

	p := jsonsender.NewJSONSender("NOTIFICATION:", &buf, notification.SeverityWarning)

	m1 := &sender.Message{
		Subject:  "test subject 1",
		Body:     "test body 1",
		Severity: notification.SeverityVerbose,
	}
	m2 := &sender.Message{
		Subject:  "test subject 2",
		Body:     "test body 2",
		Severity: notification.SeverityWarning,
	}
	m3 := &sender.Message{
		Subject:  "test subject 3",
		Body:     "test body 3",
		Severity: notification.SeverityError,
	}
	require.NoError(t, p.Send(ctx, m1)) // will be ignored
	require.NoError(t, p.Send(ctx, m2))
	require.NoError(t, p.Send(ctx, m3))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")

	require.Equal(t,
		[]string{
			"NOTIFICATION:{\"subject\":\"test subject 2\",\"severity\":10,\"body\":\"test body 2\"}",
			"NOTIFICATION:{\"subject\":\"test subject 3\",\"severity\":20,\"body\":\"test body 3\"}",
		}, lines)
}
