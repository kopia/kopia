package azure

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/blob"
)

func TestUserAgent(t *testing.T) {
	ctx := t.Context()
	container := "testContainer"
	storageAccount := "testAccount"
	storageKey := base64.StdEncoding.EncodeToString([]byte("testKey"))

	uaChannel := make(chan string, 1)

	handler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("test"))

		uaChannel <- r.Header.Get("User-Agent")
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

	ua := <-uaChannel
	require.Contains(t, ua, blob.ApplicationID)
}
