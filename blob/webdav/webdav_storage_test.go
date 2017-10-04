package webdav

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"golang.org/x/net/webdav"

	"github.com/kopia/kopia/internal/storagetesting"
)

func TestWebDAVStorage(t *testing.T) {
	t.Skip()
	tmpDir, _ := ioutil.TempDir("", "webdav")
	defer os.RemoveAll(tmpDir)

	mux := http.NewServeMux()
	mux.Handle("/", &webdav.Handler{
		FileSystem: webdav.Dir(tmpDir),
		LockSystem: webdav.NewMemLS(),
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	// Test varioush shard configurations.
	for _, shardSpec := range [][]int{
		[]int{1},
		[]int{3, 3},
		[]int{2},
		[]int{1, 1},
		[]int{1, 2},
		[]int{2, 2, 2},
	} {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Errorf("can't remove all: %q", tmpDir)
		}
		os.MkdirAll(tmpDir, 0700)

		r, err := New(context.Background(), &Options{
			URL:             server.URL,
			DirectoryShards: shardSpec,
		})

		if r == nil || err != nil {
			t.Errorf("unexpected result: %v %v", r, err)
		}

		storagetesting.VerifyStorage(t, r)
	}
}
