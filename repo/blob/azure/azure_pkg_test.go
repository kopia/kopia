package azure

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAzureUserAgentHeader(t *testing.T) {
	const wantAppID = "kopia"

	// a local http server captures the request
	var gotUserAgent string
	var called bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		gotUserAgent = r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
	}))

	t.Cleanup(server.Close)

	opts := &Options{
		SASToken: "dummy-token",
	}

	t.Log("server URL", server.URL)

	u, err := url.Parse(server.URL)
	require.NoError(t, err)

	c, err := getAZService(opts, u)
	require.NoError(t, err)

	_, err = c.ServiceClient().GetAccountInfo(t.Context(), nil)

	require.NoError(t, err)
	require.True(t, called)
	require.Contains(t, gotUserAgent, wantAppID)
}
