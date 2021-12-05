package server_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/testutil"
)

func TestPathsAPI(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := startServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            testUIUsername,
		Password:                            testUIPassword,
	})

	require.NoError(t, err)

	dir0 := testutil.TempDirectory(t)

	req := &serverapi.ResolvePathRequest{
		Path: dir0,
	}
	resp := &serverapi.ResolvePathResponse{}
	require.NoError(t, cli.Post(ctx, "paths/resolve", req, resp))

	require.Equal(t, localSource(env, dir0), resp.SourceInfo)
}
