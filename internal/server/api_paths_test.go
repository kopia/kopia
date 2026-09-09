package server_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/servertesting"
	"github.com/kopia/kopia/internal/testutil"
)

func TestPathsAPI(t *testing.T) {
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

	dir0 := testutil.TempDirectory(t)

	req := &serverapi.ResolvePathRequest{
		Path: dir0,
	}
	resp := &serverapi.ResolvePathResponse{}
	require.NoError(t, cli.Post(ctx, "paths/resolve", req, resp))

	require.Equal(t, env.LocalPathSourceInfo(dir0), resp.SourceInfo)
}

func TestPathsAPI_RequestBodyTooLarge(t *testing.T) {
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

	// the request body limit is 100_000 bytes, so a path much longer than that
	// must be rejected before it reaches the handler.
	req := &serverapi.ResolvePathRequest{
		Path: strings.Repeat("a", 105_000),
	}
	resp := &serverapi.ResolvePathResponse{}

	err = cli.Post(ctx, "paths/resolve", req, resp)
	require.Error(t, err)

	var hsr apiclient.HTTPStatusError

	require.ErrorAs(t, err, &hsr)
	require.Equal(t, http.StatusRequestEntityTooLarge, hsr.HTTPStatusCode)
}
