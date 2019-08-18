package webdav

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"golang.org/x/net/webdav"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/logging"
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
			w.WriteHeader(401)
			w.Write([]byte("Unauthorized.\n")) //nolint:errcheck
		}
	}
}

func TestWebDAVStorageExternalServer(t *testing.T) {
	t.Parallel()
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
	tmpDir, _ := ioutil.TempDir("", "webdav")
	defer os.RemoveAll(tmpDir)

	mux := http.NewServeMux()
	mux.HandleFunc("/", basicAuth(&webdav.Handler{
		FileSystem: webdav.Dir(tmpDir),
		LockSystem: webdav.NewMemLS(),
	}))

	server := httptest.NewServer(mux)
	defer server.Close()

	// Test varioush shard configurations.
	for _, shardSpec := range [][]int{
		{1},
		{3, 3},
		{2},
		{1, 1},
		{1, 2},
		{2, 2, 2},
	} {
		shardSpec := shardSpec
		t.Run(fmt.Sprintf("shards-%v", shardSpec), func(t *testing.T) {
			if err := os.RemoveAll(tmpDir); err != nil {
				t.Errorf("can't remove all: %q", tmpDir)
			}
			os.MkdirAll(tmpDir, 0700) //nolint:errcheck

			verifyWebDAVStorage(t, server.URL, "user", "password", shardSpec)
		})
	}
}

func verifyWebDAVStorage(t *testing.T, url, username, password string, shardSpec []int) {
	ctx := context.Background()

	st, err := New(context.Background(), &Options{
		URL:             url,
		DirectoryShards: shardSpec,
		Username:        username,
		Password:        password,
	})

	if st == nil || err != nil {
		t.Errorf("unexpected result: %v %v", st, err)
	}

	st = logging.NewWrapper(st)

	if err := st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		return st.DeleteBlob(ctx, bm.BlobID)
	}); err != nil {
		t.Fatalf("unable to clear webdav storage: %v", err)
	}

	blobtesting.VerifyStorage(ctx, t, st)
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	if err := st.Close(ctx); err != nil {
		t.Fatalf("err: %v", err)
	}
}
