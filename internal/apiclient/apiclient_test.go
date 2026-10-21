package apiclient_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
)

func TestNewKopiaAPIClientEmptyCA(t *testing.T) {
	_, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                    "https://127.0.0.1:1",
		TrustedServerCACertificate: []byte{},
	})
	require.ErrorContains(t, err, "server CA certificate is empty")
}
