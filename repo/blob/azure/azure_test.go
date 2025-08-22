package azure

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/blob"
)

func TestUserAgent(t *testing.T) {
	ctx := t.Context()
	container := "testContainer"
	storageAccount := "testAccount"
	storageKey := base64.StdEncoding.EncodeToString([]byte("testKey"))

	var seenKopiaUserAgent atomic.Bool

	handler := func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("User-Agent"), blob.ApplicationID) {
			seenKopiaUserAgent.Store(true)
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("test"))
	}

	server := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(server.Close)

	opt := &Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
		DoNotUseTLS:    true,
	}
	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	client, err := getAZService(opt, serverURL.Host)
	require.NoError(t, err)
	require.NotNil(t, client)

	raw := &azStorage{
		Options:   *opt,
		container: opt.Container,
		service:   client,
	}

	// Test that the User-Agent is set correctly by calling ListBlobs.
	nonExistentPrefix := fmt.Sprintf("kopia-azure-storage-initializing-%v", clock.Now().UnixNano())
	err = raw.ListBlobs(ctx, blob.ID(nonExistentPrefix), func(_ blob.Metadata) error {
		return nil
	})
	require.Error(t, err)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		require.True(collect, seenKopiaUserAgent.Load())
	}, time.Minute, 100*time.Millisecond)
}
