package server_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/auth"
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
func startServer(ctx context.Context, t *testing.T) *repo.APIServerInfo {
	var env repotesting.Environment

	env.Setup(t)
	t.Cleanup(func() { env.Close(ctx, t) })

	s, err := server.New(ctx, server.Options{
		ConfigFile: env.ConfigFile(),
		Authorizer: auth.LegacyAuthorizerForUser,
		Authenticator: auth.CombineAuthenticators(
			auth.AuthenticateSingleUser(testUsername+"@"+testHostname, testPassword),
			auth.AuthenticateSingleUser(testUIUsername, testUIPassword),
		),
		RefreshInterval: 1 * time.Minute,
		UIUser:          testUIUsername,
	})

	s.SetRepository(ctx, env.Repository)

	// ensure we disconnect the repository before shutting down the server.
	t.Cleanup(func() { s.SetRepository(ctx, nil) })

	if err != nil {
		t.Fatal(err)
	}

	hs := httptest.NewUnstartedServer(s.GRPCRouterHandler(s.APIHandlers(true)))
	hs.EnableHTTP2 = true
	hs.StartTLS()

	t.Cleanup(hs.Close)

	serverHash := sha256.Sum256(hs.Certificate().Raw)

	return &repo.APIServerInfo{
		BaseURL:                             hs.URL,
		TrustedServerCertificateFingerprint: hex.EncodeToString(serverHash[:]),
	}
}

func TestServer_REST(t *testing.T) {
	testServer(t, true)
}

func TestServer_GRPC(t *testing.T) {
	testServer(t, false)
}

// nolint:thelper
func testServer(t *testing.T, disableGRPC bool) {
	ctx := testlogging.ContextWithLevel(t, testlogging.LevelDebug)
	apiServerInfo := startServer(ctx, t)

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
	ctx := testlogging.ContextWithLevel(t, testlogging.LevelDebug)
	apiServerInfo := startServer(ctx, t)

	if _, err := repo.OpenGRPCAPIRepository(ctx, apiServerInfo, repo.ClientOptions{
		Username: "bad-username",
		Hostname: "bad-hostname",
	}, nil, "bad-password"); err == nil {
		t.Fatal("unexpected success when connecting with invalid username")
	}
}

func TestServerUIAccessDeniedToRemoteUser(t *testing.T) {
	ctx := testlogging.ContextWithLevel(t, testlogging.LevelDebug)
	si := startServer(ctx, t)

	remoteUserClient, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             si.BaseURL,
		TrustedServerCertificateFingerprint: si.TrustedServerCertificateFingerprint,
		Username:                            testUsername + "@" + testHostname,
		Password:                            testPassword,
	})

	must(t, err)

	uiUserClient, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             si.BaseURL,
		TrustedServerCertificateFingerprint: si.TrustedServerCertificateFingerprint,
		Username:                            testUIUsername,
		Password:                            testUIPassword,
	})

	must(t, err)

	// examples of URLs and expected statuses returned when UI user calls them, but which must return 403 when
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

			if err := remoteUserClient.Get(ctx, urlSuffix, nil, nil); !errors.As(err, &hsr) || hsr.HTTPStatusCode != http.StatusForbidden {
				t.Fatalf("error returned expected to be HTTPStatusError %v, want %v", hsr.HTTPStatusCode, http.StatusForbidden)
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

	must(t, repo.WriteSession(ctx, rep, repo.WriteSessionOptions{
		Purpose: "write test",
		OnUpload: func(i int64) {
			uploaded += i
		},
	}, func(w repo.RepositoryWriter) error {
		mustGetObjectNotFound(ctx, t, w, "abcd")
		mustGetManifestNotFound(ctx, t, w, "mnosuchmanifest")
		mustManifestNotFound(t, w.DeleteManifest(ctx, manifestID2))
		mustListSnapshotCount(ctx, t, w, 0)

		result = mustWriteObject(ctx, t, w, written)

		if uploaded == 0 {
			t.Fatalf("did not report uploaded bytes")
		}

		uploaded = 0
		result2 := mustWriteObject(ctx, t, w, written)
		if uploaded != 0 {
			t.Fatalf("unexpected upload when writing duplicate object")
		}

		if result != result2 {
			t.Fatalf("two identical object with different IDs: %v vs %v", result, result2)
		}

		// verify data is read back the same.
		mustReadObject(ctx, t, w, result, written)

		ow := w.NewObjectWriter(ctx, object.WriterOptions{
			Prefix: content.ID(manifest.ContentPrefix),
		})

		_, err := ow.Write([]byte{2, 3, 4})
		must(t, err)

		_, err = ow.Result()
		if err == nil {
			t.Fatalf("unexpected success writing object with 'm' prefix")
		}

		manifestID, err = snapshot.SaveSnapshot(ctx, w, &snapshot.Manifest{
			Source:      srcInfo,
			Description: "written",
		})
		must(t, err)
		mustListSnapshotCount(ctx, t, w, 1)

		manifestID2, err = snapshot.SaveSnapshot(ctx, w, &snapshot.Manifest{
			Source:      srcInfo,
			Description: "written2",
		})
		must(t, err)
		mustListSnapshotCount(ctx, t, w, 2)

		mustReadManifest(ctx, t, w, manifestID, "written")
		mustReadManifest(ctx, t, w, manifestID2, "written2")

		must(t, w.DeleteManifest(ctx, manifestID2))
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
	must(t, err)

	result, err := ow.Result()
	must(t, err)

	return result
}

func mustReadObject(ctx context.Context, t *testing.T, r repo.Repository, oid object.ID, want []byte) {
	t.Helper()

	or, err := r.OpenObject(ctx, oid)
	must(t, err)

	data, err := ioutil.ReadAll(or)
	must(t, err)

	// verify data is read back the same.
	if diff := cmp.Diff(data, want); diff != "" {
		t.Fatalf("invalid object data, diff: %v", diff)
	}
}

func mustReadManifest(ctx context.Context, t *testing.T, r repo.Repository, manID manifest.ID, want string) {
	t.Helper()

	man, err := snapshot.LoadSnapshot(ctx, r, manID)
	must(t, err)

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

func must(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}

func mustManifestNotFound(t *testing.T, err error) {
	t.Helper()

	if !errors.Is(err, manifest.ErrNotFound) {
		t.Fatalf("invalid error %v, wanted manifest not found", err)
	}
}
