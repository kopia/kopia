package cli

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/notification/notifyprofile"
	"github.com/kopia/kopia/notification/sender"
)

func TestNotificationProfileAutocomplete(t *testing.T) {
	t.Parallel()

	var a notificationProfileFlag

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	require.Empty(t, a.listNotificationProfiles(ctx, env.Repository))
	require.NoError(t, notifyprofile.SaveProfile(ctx, env.RepositoryWriter, notifyprofile.Config{
		ProfileName: "test-profile",
		MethodConfig: sender.MethodConfig{
			Type:   "email",
			Config: map[string]string{},
		},
	}))
	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	require.Contains(t, a.listNotificationProfiles(ctx, env.Repository), "test-profile")

	a.profileName = "no-such-profile"
	require.Empty(t, a.listNotificationProfiles(ctx, env.Repository))
}
