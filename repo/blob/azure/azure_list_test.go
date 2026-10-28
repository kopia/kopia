package azure

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

// newTestStorageWithResponse returns a storage backed by a server that answers every request
// with the provided status code and body, similar to what a non-blob endpoint may return.
func newTestStorageWithResponse(t *testing.T, status int, body string) azStorage {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))

	t.Cleanup(srv.Close)

	client, err := azblob.NewClientWithNoCredential(srv.URL, nil)
	require.NoError(t, err)

	return azStorage{
		service:   client,
		container: "test-container",
	}
}

const nonBlobListResponse = `<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="http://example.invalid/"><Prefix>kopia</Prefix><Marker/><MaxResults>5000</MaxResults></EnumerationResults>`

func TestListBlobs_responseWithoutBlobList(t *testing.T) {
	ctx := testlogging.Context(t)
	az := newTestStorageWithResponse(t, http.StatusOK, nonBlobListResponse)

	err := az.ListBlobs(ctx, blob.ID("p"), func(blob.Metadata) error {
		t.Fatal("callback must not be called")

		return nil
	})

	require.ErrorContains(t, err, "list of blobs")
}

func TestListBlobs_pointInTime_responseWithoutBlobList(t *testing.T) {
	ctx := testlogging.Context(t)
	pit := &azPointInTimeStorage{
		azStorage:   newTestStorageWithResponse(t, http.StatusOK, nonBlobListResponse),
		pointInTime: clock.Now(),
	}

	err := pit.ListBlobs(ctx, blob.ID("p"), func(blob.Metadata) error {
		t.Fatal("callback must not be called")

		return nil
	})

	require.ErrorContains(t, err, "list of blobs")
}
