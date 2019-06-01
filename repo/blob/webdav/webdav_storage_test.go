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
)

func TestWebDAVStorage(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "webdav")
	defer os.RemoveAll(tmpDir)

	t.Logf("tmpDir: %v", tmpDir)

	mux := http.NewServeMux()
	mux.Handle("/", &webdav.Handler{
		FileSystem: webdav.Dir(tmpDir),
		LockSystem: webdav.NewMemLS(),
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	ctx := context.Background()

	// Test varioush shard configurations.
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
			os.MkdirAll(tmpDir, 0700) //nolint:errcheck

			r, err := New(context.Background(), &Options{
				URL:             server.URL,
				DirectoryShards: shardSpec,
			})

			if r == nil || err != nil {
				t.Errorf("unexpected result: %v %v", r, err)
			}

			blobtesting.VerifyStorage(ctx, t, r)
			blobtesting.AssertConnectionInfoRoundTrips(ctx, t, r)
			if err := r.Close(ctx); err != nil {
				t.Fatalf("err: %v", err)
			}
		})
	}
}
