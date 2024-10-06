package cli

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/repotesting"
)

func TestNotificationTemplatesAutocomplete(t *testing.T) {
	t.Parallel()

	var a notificationTemplateNameArg

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	require.Contains(t,
		a.listNotificationTemplates(ctx, env.Repository),
		"test-notification.txt")

	a.templateName = "no-such-prefix"
	require.Empty(t, a.listNotificationTemplates(ctx, env.Repository))

	a.templateName = "test-notif"
	require.Contains(t,
		a.listNotificationTemplates(ctx, env.Repository),
		"test-notification.txt")
}
