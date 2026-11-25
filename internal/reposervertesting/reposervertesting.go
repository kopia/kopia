// Package reposervertesting provides helpers for testing the gRPC repository server.
package reposervertesting

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/reposerver"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/notification/notifytemplate"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

const (
	TestUsername = "foo"
	TestHostname = "bar"
	TestPassword = "123"
)

type testProvider struct {
	rep   repo.Repository
	authn auth.Authenticator
	authz auth.Authorizer
}

func (p *testProvider) Repository() repo.Repository {
	return p.rep
}

func (p *testProvider) Authenticator() auth.Authenticator {
	return p.authn
}

func (p *testProvider) Authorizer() auth.Authorizer {
	return p.authz
}

func (*testProvider) NotificationTemplateOptions() notifytemplate.Options {
	return notifytemplate.Options{}
}

// Start starts an HTTP/2 test server that exposes only the gRPC repository
// service and returns repo.APIServerInfo.
func Start(t *testing.T, env *repotesting.Environment) *repo.APIServerInfo {
	t.Helper()

	s := reposerver.New(&testProvider{
		rep:   env.Repository,
		authn: auth.AuthenticateSingleUser(TestUsername+"@"+TestHostname, TestPassword),
		authz: auth.LegacyAuthorizer(),
	}, reposerver.Options{})

	hs := httptest.NewUnstartedServer(s.GRPCRouterHandler(http.NotFoundHandler()))
	hs.EnableHTTP2 = true
	hs.StartTLS()

	serverHash := sha256.Sum256(hs.Certificate().Raw)

	asi := &repo.APIServerInfo{
		BaseURL:                             hs.URL,
		TrustedServerCertificateFingerprint: hex.EncodeToString(serverHash[:]),
	}

	t.Cleanup(func() {
		s.ShutdownGRPCServer()
		hs.Close()
	})

	return asi
}

// ConnectAndOpen connects a repo client to the gRPC test server and opens it.
func ConnectAndOpen(t *testing.T, ctx context.Context, asi *repo.APIServerInfo, rco repo.ClientOptions, caching content.CachingOptions, password string, opt *repo.Options) (repo.Repository, error) {
	t.Helper()

	configFile := filepath.Join(t.TempDir(), "tmp.config")

	if err := repo.ConnectAPIServer(ctx, configFile, asi, password, &repo.ConnectOptions{
		ClientOptions:  rco,
		CachingOptions: caching,
	}); err != nil {
		return nil, err
	}

	t.Cleanup(func() {
		repo.Disconnect(testlogging.ContextForCleanup(t), configFile)
	})

	return repo.Open(ctx, configFile, password, opt)
}
