package server_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/servertesting"
	"github.com/kopia/kopia/notification/notifyprofile"
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/notification/sender/testsender"
)

func TestNotificationProfile(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, err)

	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	var profiles []notifyprofile.Config

	require.NoError(t, cli.Get(ctx, "notificationProfiles", nil, &profiles))
	require.Empty(t, profiles)

	// define new profile
	require.NoError(t, cli.Post(ctx, "notificationProfiles", &notifyprofile.Config{
		ProfileName: "profile1",
		MethodConfig: sender.MethodConfig{
			Type: "testsender",
			Config: testsender.Options{
				Format: "txt",
			},
		},
		MinSeverity: 3,
	}, &serverapi.Empty{}))

	// define invalid profile
	require.ErrorContains(t, cli.Post(ctx, "notificationProfiles", &notifyprofile.Config{
		ProfileName: "profile2",
		MethodConfig: sender.MethodConfig{
			Type: "no-such-type",
			Config: testsender.Options{
				Format: "txt",
			},
		},
		MinSeverity: 3,
	}, &serverapi.Empty{}), "malformed request body")

	var cfg notifyprofile.Config

	// get profile and verify
	require.NoError(t, cli.Get(ctx, "notificationProfiles/profile1", nil, &cfg))
	require.Equal(t, "profile1", cfg.ProfileName)
	require.Equal(t, sender.Method("testsender"), cfg.MethodConfig.Type)

	opt, ok := cfg.MethodConfig.Config.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "txt", opt["format"])

	// get non-existent profile
	require.ErrorContains(t, cli.Get(ctx, "notificationProfiles/profile2", nil, &cfg), "profile not found")

	// list profiles
	require.NoError(t, cli.Get(ctx, "notificationProfiles", nil, &profiles))
	require.Len(t, profiles, 1)
	require.Equal(t, "profile1", profiles[0].ProfileName)

	// delete the profile, ensure idempotent
	require.NoError(t, cli.Delete(ctx, "notificationProfiles/profile1", nil, nil, &serverapi.Empty{}))
	require.NoError(t, cli.Delete(ctx, "notificationProfiles/profile1", nil, nil, &serverapi.Empty{}))

	// verify it's gone
	require.NoError(t, cli.Get(ctx, "notificationProfiles", nil, &profiles))
	require.Empty(t, profiles)
}
