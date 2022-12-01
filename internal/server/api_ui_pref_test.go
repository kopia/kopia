package server_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/servertesting"
)

func TestUIPreferences(t *testing.T) {
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

	var p, p2 serverapi.UIPreferences

	require.NoError(t, cli.Get(ctx, "ui-preferences", nil, &p))
	require.Equal(t, "", p.Theme)
	p.Theme = "dark"

	require.NoError(t, cli.Put(ctx, "ui-preferences", &p, &serverapi.Empty{}))
	require.NoError(t, cli.Get(ctx, "ui-preferences", nil, &p2))
	require.Equal(t, p, p2)
}
