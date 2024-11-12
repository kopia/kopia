package server_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/servertesting"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/notification/notifyprofile"
	"github.com/kopia/kopia/notification/notifytemplate"
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/notification/sender/webhook"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

const (
	testPathname = "/tmp/path"

	maxCacheSizeBytes = 1e6
)

func TestServer(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	apiServerInfo := servertesting.StartServer(t, env, true)

	ctx2, cancel := context.WithCancel(ctx)

	rep, err := servertesting.ConnectAndOpenAPIServer(t, ctx2, apiServerInfo, repo.ClientOptions{
		Username: servertesting.TestUsername,
		Hostname: servertesting.TestHostname,
	}, content.CachingOptions{
		CacheDirectory:        testutil.TempDirectory(t),
		ContentCacheSizeBytes: maxCacheSizeBytes,
	}, servertesting.TestPassword, &repo.Options{})

	// cancel immediately to ensure we did not spawn goroutines that depend on ctx inside
	// repo.OpenAPIServer()
	cancel()

	require.NoError(t, err)

	defer rep.Close(ctx)

	remoteRepositoryTest(ctx, t, rep)
	remoteRepositoryNotificationTest(t, ctx, rep, env.RepositoryWriter)
}

func TestGRPCServer_AuthenticationError(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	apiServerInfo := servertesting.StartServer(t, env, true)

	if _, err := servertesting.ConnectAndOpenAPIServer(t, ctx, apiServerInfo, repo.ClientOptions{
		Username: "bad-username",
		Hostname: "bad-hostname",
	}, content.CachingOptions{}, "bad-password", &repo.Options{}); err == nil {
		t.Fatal("unexpected success when connecting with invalid username")
	}
}

//nolint:gocyclo
func TestServerUIAccessDeniedToRemoteUser(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	si := servertesting.StartServer(t, env, true)

	remoteUserClient, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             si.BaseURL,
		TrustedServerCertificateFingerprint: si.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUsername + "@" + servertesting.TestHostname,
		Password:                            servertesting.TestPassword,
	})
	require.NoError(t, err)

	uiUserWithoutCSRFToken, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             si.BaseURL,
		TrustedServerCertificateFingerprint: si.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, err)

	uiUserClient, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             si.BaseURL,
		TrustedServerCertificateFingerprint: si.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
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

//nolint:thelper
func remoteRepositoryTest(ctx context.Context, t *testing.T, rep repo.Repository) {
	mustListSnapshotCount(ctx, t, rep, 0)
	mustGetObjectNotFound(ctx, t, rep, mustParseObjectID(t, "abcd"))
	mustGetManifestNotFound(ctx, t, rep, "mnosuchmanifest")
	mustPrefetchObjectsNotFound(ctx, t, rep, mustParseObjectID(t, "abcd"))

	var (
		result                  object.ID
		manifestID, manifestID2 manifest.ID
		written                 = make([]byte, 100000)
		srcInfo                 = snapshot.SourceInfo{
			Host:     servertesting.TestHostname,
			UserName: servertesting.TestUsername,
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
		mustGetObjectNotFound(ctx, t, w, mustParseObjectID(t, "abcd"))
		mustGetManifestNotFound(ctx, t, w, "mnosuchmanifest")
		mustListSnapshotCount(ctx, t, w, 0)
		mustPrefetchObjectsNotFound(ctx, t, rep, mustParseObjectID(t, "abcd"))

		result = mustWriteObject(ctx, t, w, written)

		require.NoError(t, w.Flush(ctx))

		if uploaded == 0 {
			return errors.New("did not report uploaded bytes")
		}

		uploaded = 0
		result2 := mustWriteObject(ctx, t, w, written)
		require.NoError(t, w.Flush(ctx))

		if uploaded != 0 {
			return errors.New("unexpected upload when writing duplicate object")
		}

		if result != result2 {
			return errors.Errorf("two identical object with different IDs: %v vs %v", result, result2)
		}

		// verify data is read back the same.
		mustPrefetchObjects(ctx, t, w, result)
		mustReadObject(ctx, t, w, result, written)

		ow := w.NewObjectWriter(ctx, object.WriterOptions{
			Prefix: manifest.ContentPrefix,
		})

		_, err := ow.Write([]byte{2, 3, 4})
		require.NoError(t, err)

		_, err = ow.Result()
		if err == nil {
			return errors.New("unexpected success writing object with 'm' prefix")
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
	mustPrefetchObjects(ctx, t, rep, result)
}

//nolint:thelper
func remoteRepositoryNotificationTest(t *testing.T, ctx context.Context, rep repo.Repository, rw repo.RepositoryWriter) {
	require.Implements(t, (*repo.RemoteNotifications)(nil), rep)

	mux := http.NewServeMux()

	var numRequestsReceived atomic.Int32

	mux.HandleFunc("/some-path", func(w http.ResponseWriter, r *http.Request) {
		numRequestsReceived.Add(1)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	require.NoError(t, notifyprofile.SaveProfile(ctx, rw, notifyprofile.Config{
		ProfileName: "my-profile",
		MethodConfig: sender.MethodConfig{
			Type: "webhook",
			Config: &webhook.Options{
				Endpoint: server.URL + "/some-path",
				Method:   "POST",
			},
		},
	}))
	require.NoError(t, rw.Flush(ctx))

	notification.Send(ctx, rep, notifytemplate.TestNotification, nil, notification.SeverityError, notifytemplate.DefaultOptions)
	require.Equal(t, int32(1), numRequestsReceived.Load())

	// another webhook which fails

	require.NoError(t, notifyprofile.SaveProfile(ctx, rw, notifyprofile.Config{
		ProfileName: "my-profile",
		MethodConfig: sender.MethodConfig{
			Type: "webhook",
			Config: &webhook.Options{
				Endpoint: server.URL + "/some-nonexistent-path",
				Method:   "POST",
			},
		},
	}))

	require.NoError(t, rw.Flush(ctx))
	notification.Send(ctx, rep, notifytemplate.TestNotification, nil, notification.SeverityError, notifytemplate.DefaultOptions)
	require.Equal(t, int32(1), numRequestsReceived.Load())
}

func mustWriteObject(ctx context.Context, t *testing.T, w repo.RepositoryWriter, data []byte) object.ID {
	t.Helper()

	ow := w.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})

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

func mustPrefetchObjects(ctx context.Context, t *testing.T, r repo.Repository, oid ...object.ID) {
	t.Helper()

	contents, err := r.PrefetchObjects(ctx, oid, "")
	require.NoError(t, err)
	require.NotEmpty(t, contents)
}

func mustPrefetchObjectsNotFound(ctx context.Context, t *testing.T, r repo.Repository, oid ...object.ID) {
	t.Helper()

	contents, err := r.PrefetchObjects(ctx, oid, "")
	require.NoError(t, err)
	require.Empty(t, contents)
}

func mustGetManifestNotFound(ctx context.Context, t *testing.T, r repo.Repository, manID manifest.ID) {
	t.Helper()

	_, err := r.GetManifest(ctx, manID, nil)
	mustManifestNotFound(t, err)
}

func mustListSnapshotCount(ctx context.Context, t *testing.T, rep repo.Repository, wantCount int) {
	t.Helper()

	snaps, err := snapshot.ListSnapshots(ctx, rep, snapshot.SourceInfo{
		UserName: servertesting.TestUsername,
		Host:     servertesting.TestHostname,
		Path:     testPathname,
	})
	require.NoError(t, err)

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

//nolint:unparam
func mustParseObjectID(t *testing.T, s string) object.ID {
	t.Helper()

	id, err := object.ParseID(s)
	require.NoError(t, err)

	return id
}
