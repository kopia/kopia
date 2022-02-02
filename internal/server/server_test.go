package server_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/passwordpersist"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/server"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

const (
	testUsername = "foo"
	testHostname = "bar"
	testPassword = "123"
	testPathname = "/tmp/path"

	testUIUsername = "ui-user"
	testUIPassword = "123456"

	maxCacheSizeBytes = 1e6
)

// nolint:thelper
func startServer(t *testing.T, env *repotesting.Environment, tls bool) *repo.APIServerInfo {
	ctx := testlogging.Context(t)

	s, err := server.New(ctx, &server.Options{
		ConfigFile:      env.ConfigFile(),
		PasswordPersist: passwordpersist.File(),
		Authorizer:      auth.LegacyAuthorizer(),
		Authenticator: auth.CombineAuthenticators(
			auth.AuthenticateSingleUser(testUsername+"@"+testHostname, testPassword),
			auth.AuthenticateSingleUser(testUIUsername, testUIPassword),
		),
		RefreshInterval:   1 * time.Minute,
		UIUser:            testUIUsername,
		UIPreferencesFile: filepath.Join(testutil.TempDirectory(t), "ui-pref.json"),
	})

	require.NoError(t, err)

	s.SetRepository(ctx, env.Repository)

	// ensure we disconnect the repository before shutting down the server.
	t.Cleanup(func() { s.SetRepository(ctx, nil) })

	if err != nil {
		t.Fatal(err)
	}

	asi := &repo.APIServerInfo{}

	m := mux.NewRouter()
	s.SetupHTMLUIAPIHandlers(m)
	s.SetupRepositoryAPIHandlers(m)
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

func TestServer_REST(t *testing.T) {
	testServer(t, true)
}

func TestServer_GRPC(t *testing.T) {
	testServer(t, false)
}

// nolint:thelper
func testServer(t *testing.T, disableGRPC bool) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	apiServerInfo := startServer(t, env, true)

	apiServerInfo.DisableGRPC = disableGRPC

	rep, err := repo.OpenAPIServer(ctx, apiServerInfo, repo.ClientOptions{
		Username: testUsername,
		Hostname: testHostname,
	}, &content.CachingOptions{
		CacheDirectory:    testutil.TempDirectory(t),
		MaxCacheSizeBytes: maxCacheSizeBytes,
	}, testPassword)
	if err != nil {
		t.Fatal(err)
	}

	defer rep.Close(ctx)

	remoteRepositoryTest(ctx, t, rep)
}

func TestGPRServer_AuthenticationError(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	apiServerInfo := startServer(t, env, true)

	if _, err := repo.OpenGRPCAPIRepository(ctx, apiServerInfo, repo.ClientOptions{
		Username: "bad-username",
		Hostname: "bad-hostname",
	}, nil, "bad-password"); err == nil {
		t.Fatal("unexpected success when connecting with invalid username")
	}
}

// nolint:gocyclo
func TestServerUIAccessDeniedToRemoteUser(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	si := startServer(t, env, true)

	remoteUserClient, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             si.BaseURL,
		TrustedServerCertificateFingerprint: si.TrustedServerCertificateFingerprint,
		Username:                            testUsername + "@" + testHostname,
		Password:                            testPassword,
	})
	require.NoError(t, err)

	uiUserWithoutCSRFToken, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             si.BaseURL,
		TrustedServerCertificateFingerprint: si.TrustedServerCertificateFingerprint,
		Username:                            testUIUsername,
		Password:                            testUIPassword,
	})

	require.NoError(t, err)

	uiUserClient, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             si.BaseURL,
		TrustedServerCertificateFingerprint: si.TrustedServerCertificateFingerprint,
		Username:                            testUIUsername,
		Password:                            testUIPassword,
	})

	require.NoError(t, err)

	require.NoError(t, uiUserClient.FetchCSRFTokenForTesting(ctx))
	// do not call uiUserWithoutCSRFToken.FetchCSRFTokenForTesting()

	// examples of URLs and expected statuses returned when UI user calls them, but which must return 401 due to missing CSRF token
	// remote user calls them.
	getUrls := map[string]int{
		"mounts":          http.StatusOK,
		"repo/algorithms": http.StatusOK,
		"objects/abcd":    http.StatusNotFound,
		"tasks-summary":   http.StatusOK,
		"tasks":           http.StatusOK,
		"policy":          http.StatusBadRequest,
	}

	for urlSuffix, wantStatus := range getUrls {
		urlSuffix := urlSuffix
		wantStatus := wantStatus

		t.Run(urlSuffix, func(t *testing.T) {
			var hsr apiclient.HTTPStatusError

			wantFailure := http.StatusUnauthorized // 401

			if urlSuffix == "objects/abcd" {
				// this is a special one that does not require CSRF token but will still fail with 403
				wantFailure = http.StatusForbidden
			}

			if err := remoteUserClient.Get(ctx, urlSuffix, nil, nil); !errors.As(err, &hsr) || (hsr.HTTPStatusCode != wantFailure) {
				t.Fatalf("error returned expected to be HTTPStatusError %v, want %v", hsr.HTTPStatusCode, wantFailure)
			}

			if wantStatus == http.StatusOK {
				if err := uiUserClient.Get(ctx, urlSuffix, nil, nil); err != nil {
					t.Fatalf("expected success, got %v", err)
				}
			} else if err := uiUserClient.Get(ctx, urlSuffix, nil, nil); !errors.As(err, &hsr) || hsr.HTTPStatusCode != wantStatus {
				t.Fatalf("error returned expected to be HTTPStatusError %v, want %v", hsr.HTTPStatusCode, wantStatus)
			}

			// objects/abcd does not require CSRF token so will fail with 404 instead of 403.
			// This is fine since this is a side-effect-free GET method so same-origin policy
			// will protect access to data.
			if urlSuffix == "objects/abcd" {
				wantFailure = http.StatusNotFound
			}

			if err := uiUserWithoutCSRFToken.Get(ctx, urlSuffix, nil, nil); !errors.As(err, &hsr) || (hsr.HTTPStatusCode != wantFailure) {
				t.Fatalf("error returned expected to be HTTPStatusError %v, want %v", hsr.HTTPStatusCode, wantFailure)
			}

			if wantStatus == http.StatusOK {
				if err := uiUserClient.Get(ctx, urlSuffix, nil, nil); err != nil {
					t.Fatalf("expected success, got %v", err)
				}
			} else if err := uiUserClient.Get(ctx, urlSuffix, nil, nil); !errors.As(err, &hsr) || hsr.HTTPStatusCode != wantStatus {
				t.Fatalf("error returned expected to be HTTPStatusError %v, want %v", hsr.HTTPStatusCode, wantStatus)
			}
		})
	}
}

// nolint:thelper
func remoteRepositoryTest(ctx context.Context, t *testing.T, rep repo.Repository) {
	mustListSnapshotCount(ctx, t, rep, 0)
	mustGetObjectNotFound(ctx, t, rep, "abcd")
	mustGetManifestNotFound(ctx, t, rep, "mnosuchmanifest")

	var (
		result                  object.ID
		manifestID, manifestID2 manifest.ID
		written                 = []byte{1, 2, 3}
		srcInfo                 = snapshot.SourceInfo{
			Host:     testHostname,
			UserName: testUsername,
			Path:     testPathname,
		}
	)

	var uploaded int64

	require.NoError(t, repo.WriteSession(ctx, rep, repo.WriteSessionOptions{
		Purpose: "write test",
		OnUpload: func(i int64) {
			uploaded += i
		},
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		mustGetObjectNotFound(ctx, t, w, "abcd")
		mustGetManifestNotFound(ctx, t, w, "mnosuchmanifest")
		mustListSnapshotCount(ctx, t, w, 0)

		result = mustWriteObject(ctx, t, w, written)

		require.NoError(t, w.Flush(ctx))

		if uploaded == 0 {
			return errors.Errorf("did not report uploaded bytes")
		}

		uploaded = 0
		result2 := mustWriteObject(ctx, t, w, written)
		require.NoError(t, w.Flush(ctx))

		if uploaded != 0 {
			return errors.Errorf("unexpected upload when writing duplicate object")
		}

		if result != result2 {
			return errors.Errorf("two identical object with different IDs: %v vs %v", result, result2)
		}

		// verify data is read back the same.
		mustReadObject(ctx, t, w, result, written)

		ow := w.NewObjectWriter(ctx, object.WriterOptions{
			Prefix: content.ID(manifest.ContentPrefix),
		})

		_, err := ow.Write([]byte{2, 3, 4})
		require.NoError(t, err)

		_, err = ow.Result()
		if err == nil {
			return errors.Errorf("unexpected success writing object with 'm' prefix")
		}

		manifestID, err = snapshot.SaveSnapshot(ctx, w, &snapshot.Manifest{
			Source:      srcInfo,
			Description: "written",
		})
		require.NoError(t, err)
		mustListSnapshotCount(ctx, t, w, 1)

		manifestID2, err = snapshot.SaveSnapshot(ctx, w, &snapshot.Manifest{
			Source:      srcInfo,
			Description: "written2",
		})
		require.NoError(t, err)
		mustListSnapshotCount(ctx, t, w, 2)

		mustReadManifest(ctx, t, w, manifestID, "written")
		mustReadManifest(ctx, t, w, manifestID2, "written2")

		require.NoError(t, w.DeleteManifest(ctx, manifestID2))
		mustListSnapshotCount(ctx, t, w, 1)

		mustGetManifestNotFound(ctx, t, w, manifestID2)
		mustReadManifest(ctx, t, w, manifestID, "written")

		return nil
	}))

	// data and manifest written in a session can be read outside of it
	mustReadObject(ctx, t, rep, result, written)
	mustReadManifest(ctx, t, rep, manifestID, "written")
	mustGetManifestNotFound(ctx, t, rep, manifestID2)
	mustListSnapshotCount(ctx, t, rep, 1)
}

func mustWriteObject(ctx context.Context, t *testing.T, w repo.RepositoryWriter, data []byte) object.ID {
	t.Helper()

	ow := w.NewObjectWriter(ctx, object.WriterOptions{})

	_, err := ow.Write(data)
	require.NoError(t, err)

	result, err := ow.Result()
	require.NoError(t, err)

	return result
}

func mustReadObject(ctx context.Context, t *testing.T, r repo.Repository, oid object.ID, want []byte) {
	t.Helper()

	or, err := r.OpenObject(ctx, oid)
	require.NoError(t, err)

	data, err := io.ReadAll(or)
	require.NoError(t, err)

	// verify data is read back the same.
	if diff := cmp.Diff(data, want); diff != "" {
		t.Fatalf("invalid object data, diff: %v", diff)
	}
}

func mustReadManifest(ctx context.Context, t *testing.T, r repo.Repository, manID manifest.ID, want string) {
	t.Helper()

	man, err := snapshot.LoadSnapshot(ctx, r, manID)
	require.NoError(t, err)

	// verify data is read back the same.
	if diff := cmp.Diff(man.Description, want); diff != "" {
		t.Fatalf("invalid manifest data, diff: %v", diff)
	}
}

func mustGetObjectNotFound(ctx context.Context, t *testing.T, r repo.Repository, oid object.ID) {
	t.Helper()

	if _, err := r.OpenObject(ctx, oid); !errors.Is(err, object.ErrObjectNotFound) {
		t.Fatalf("unexpected non-existent object error: %v", err)
	}
}

func mustGetManifestNotFound(ctx context.Context, t *testing.T, r repo.Repository, manID manifest.ID) {
	t.Helper()

	_, err := r.GetManifest(ctx, manID, nil)
	mustManifestNotFound(t, err)
}

func mustListSnapshotCount(ctx context.Context, t *testing.T, rep repo.Repository, wantCount int) {
	t.Helper()

	snaps, err := snapshot.ListSnapshots(ctx, rep, snapshot.SourceInfo{
		UserName: testUsername,
		Host:     testHostname,
		Path:     testPathname,
	})
	if err != nil {
		t.Fatal(err)
	}

	if got, want := len(snaps), wantCount; got != want {
		t.Fatalf("unexpected number of snapshots: %v, want %v", got, want)
	}
}

func mustManifestNotFound(t *testing.T, err error) {
	t.Helper()

	if !errors.Is(err, manifest.ErrNotFound) {
		t.Fatalf("invalid error %v, wanted manifest not found", err)
	}
}
