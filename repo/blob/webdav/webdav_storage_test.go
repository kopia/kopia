package webdav

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/webdav"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sharded"
)

func basicAuth(h http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if user, passwd, ok := r.BasicAuth(); ok {
			if user == "user" && passwd == "password" {
				h.ServeHTTP(w, r)
				return
			}

			http.Error(w, "not authorized", http.StatusForbidden)
		} else {
			w.Header().Set("WWW-Authenticate", `Basic realm="testing"`)
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Unauthorized.\n"))
		}
	}
}

func TestWebDAVStorageExternalServer(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	testURL := os.Getenv("KOPIA_WEBDAV_TEST_URL")
	if testURL == "" {
		t.Skip("KOPIA_WEBDAV_TEST_URL not provided")
	}

	testUsername := os.Getenv("KOPIA_WEBDAV_TEST_USERNAME")
	if testUsername == "" {
		t.Skip("KOPIA_WEBDAV_TEST_USERNAME not provided")
	}

	testPassword := os.Getenv("KOPIA_WEBDAV_TEST_PASSWORD")
	if testPassword == "" {
		t.Skip("KOPIA_WEBDAV_TEST_PASSWORD not provided")
	}

	verifyWebDAVStorage(t, testURL, testUsername, testPassword, nil)
}

func TestWebDAVStorageBuiltInServer(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	tmpDir := testutil.TempDirectory(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/", basicAuth(&webdav.Handler{
		FileSystem: webdav.Dir(tmpDir),
		LockSystem: webdav.NewMemLS(),
	}))

	server := httptest.NewServer(mux)
	defer server.Close()

	// Test various shard configurations.
	for _, shardSpec := range [][]int{
		{1},
		{3, 3},
		{2},
		{1, 1},
		{1, 2},
		{2, 2, 2},
	} {
		t.Run(fmt.Sprintf("shards-%v", shardSpec), func(t *testing.T) {
			if err := os.RemoveAll(tmpDir); err != nil {
				t.Errorf("can't remove all: %q", tmpDir)
			}
			os.MkdirAll(tmpDir, 0o700)

			verifyWebDAVStorage(t, server.URL, "user", "password", shardSpec)
		})
	}
}

func TestWebDAVStorageBuiltInServerWithMissingAsForbidden(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	tmpDir := testutil.TempDirectory(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/", transformMissingPUTs(basicAuth(&webdav.Handler{
		FileSystem: webdav.Dir(tmpDir),
		LockSystem: webdav.NewMemLS(),
	})))

	server := httptest.NewServer(mux)
	defer server.Close()

	verifyWebDAVStorage(t, server.URL, "user", "password", []int{1})
}

// transformMissingPUTs changes not found responses into forbidden responses.
func transformMissingPUTs(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Passthrough non-PUT methods
		if r.Method != http.MethodPut {
			next.ServeHTTP(w, r)
			return
		}

		// Intercept all PUT methods
		rec := httptest.NewRecorder()
		rec.Body = &bytes.Buffer{}
		next.ServeHTTP(rec, r)

		result := rec.Result()
		defer result.Body.Close()

		// Change the status code to forbidden if returned as not found
		if result.StatusCode == http.StatusNotFound {
			w.WriteHeader(http.StatusForbidden)
		} else {
			// Passthrough recorded response headers, status code, and body
			for header, values := range rec.Header() {
				w.Header()[header] = values
			}
			w.WriteHeader(result.StatusCode)
			io.Copy(w, result.Body)
		}
	}
}

//nolint:thelper
func verifyWebDAVStorage(t *testing.T, url, username, password string, shardSpec []int) {
	ctx := testlogging.Context(t)

	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	st, err := New(newctx, &Options{
		URL: url,
		Options: sharded.Options{
			DirectoryShards: shardSpec,
		},
		Username: username,
		Password: password,
	}, false)

	cancel()

	if st == nil || err != nil {
		t.Errorf("unexpected result: %v %v", st, err)
	}

	if err := st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		return st.DeleteBlob(ctx, bm.BlobID)
	}); err != nil {
		t.Fatalf("unable to clear webdav storage: %v", err)
	}

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))

	if err := st.Close(ctx); err != nil {
		t.Fatalf("err: %v", err)
	}
}
