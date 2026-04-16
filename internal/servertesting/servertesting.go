// Package servertesting provides helpers for launching and testing Kopia server.
package servertesting

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/passwordpersist"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/server"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

const (
	TestUsername = "foo"
	TestHostname = "bar"
	TestPassword = "123"

	TestUIUsername = "ui-user"
	TestUIPassword = "123456"
)

// StartServer starts a test server and returns APIServerInfo.
func StartServer(t *testing.T, env *repotesting.Environment, tls bool) *repo.APIServerInfo {
	t.Helper()

	return StartServerContext(testlogging.Context(t), t, env, tls)
}

// StartServerContext starts a test server with a given root context and returns APIServerInfo.
func StartServerContext(ctx context.Context, t *testing.T, env *repotesting.Environment, tls bool) *repo.APIServerInfo {
	t.Helper()

	s, err := server.New(ctx, &server.Options{
		ConfigFile:      env.ConfigFile(),
		PasswordPersist: passwordpersist.File(),
		Authorizer:      auth.LegacyAuthorizer(),
		Authenticator: auth.CombineAuthenticators(
			auth.AuthenticateSingleUser(TestUsername+"@"+TestHostname, TestPassword),
			auth.AuthenticateSingleUser(TestUIUsername, TestUIPassword),
		),
		RefreshInterval:   1 * time.Minute,
		UIUser:            TestUIUsername,
		UIPreferencesFile: filepath.Join(testutil.TempDirectory(t), "ui-pref.json"),
	})

	require.NoError(t, err)

	s.SetRepository(ctx, env.Repository)

	// ensure we disconnect the repository before shutting down the server.
	t.Cleanup(func() { s.SetRepository(ctx, nil) })

	require.NoError(t, err)

	asi := &repo.APIServerInfo{}

	m := mux.NewRouter()
	s.SetupHTMLUIAPIHandlers(m)
	s.SetupControlAPIHandlers(m)
	s.ServeStaticFiles(m, server.AssetFile())

	hs := httptest.NewUnstartedServer(s.GRPCRouterHandler(m))
	if tls {
		hs.EnableHTTP2 = true
		hs.StartTLS()
		serverHash := sha256.Sum256(hs.Certificate().Raw)
		asi.BaseURL = hs.URL
		asi.TrustedServerCertificateFingerprint = hex.EncodeToString(serverHash[:])
	} else {
		hs.Start()
		asi.BaseURL = hs.URL
	}

	t.Cleanup(hs.Close)

	return asi
}

// ConnectAndOpenAPIServer creates temporary config file and to and opens API server for testing.
func ConnectAndOpenAPIServer(t *testing.T, ctx context.Context, asi *repo.APIServerInfo, rco repo.ClientOptions, caching content.CachingOptions, password string, opt *repo.Options) (repo.Repository, error) {
	t.Helper()

	configFile := filepath.Join(t.TempDir(), "tmp.config")

	if err := repo.ConnectAPIServer(ctx, configFile, asi, password, &repo.ConnectOptions{
		ClientOptions:  rco,
		CachingOptions: caching,
	}); err != nil {
		return nil, err
	}

	t.Cleanup(func() {
		repo.Disconnect(ctx, configFile)
	})

	//
	return repo.Open(ctx, configFile, password, opt)
}
