package azure_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/azure"
)

const (
	testContainerEnv      = "KOPIA_AZURE_TEST_CONTAINER"
	testStorageAccountEnv = "KOPIA_AZURE_TEST_STORAGE_ACCOUNT"
	testStorageKeyEnv     = "KOPIA_AZURE_TEST_STORAGE_KEY"
)

func getEnvOrSkip(t *testing.T, name string) string {
	value, ok := os.LookupEnv(name)
	if !ok {
		t.Skip(fmt.Sprintf("%s not provided", name))
	}

	return value
}

func createContainer(t *testing.T, container, storageAccount, storageKey string) {
	credential, err := azblob.NewSharedKeyCredential(storageAccount, storageKey)
	if err != nil {
		t.Fatalf("failed to create Azure credentials: %v", err)
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", storageAccount))
	if err != nil {
		t.Fatalf("failed to parse container URL: %v", err)
	}

	serviceURL := azblob.NewServiceURL(*u, p)
	containerURL := serviceURL.NewContainerURL(container)

	_, err = containerURL.Create(context.Background(), azblob.Metadata{}, azblob.PublicAccessNone)
	if err == nil {
		return
	}

	// return if already exists
	if stgErr, ok := err.(azblob.StorageError); ok {
		if stgErr.ServiceCode() == azblob.ServiceCodeContainerAlreadyExists {
			return
		}
	}

	t.Fatalf("failed to create blob storage container: %v", err)
}

func TestAzureStorage(t *testing.T) {
	t.Parallel()
	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	// create container if does not exist
	createContainer(t, container, storageAccount, storageKey)

	data := make([]byte, 8)
	rand.Read(data) //nolint:errcheck

	ctx := context.Background()
	st, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
		Prefix:         fmt.Sprintf("test-%v-%x-", time.Now().Unix(), data),
	})

	if err != nil {
		t.Fatalf("unable to connect to Azure: %v", err)
	}

	if err := st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		return st.DeleteBlob(ctx, bm.BlobID)
	}); err != nil {
		t.Fatalf("unable to clear Azure blob container: %v", err)
	}

	blobtesting.VerifyStorage(ctx, t, st)
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)

	// delete everything again
	if err := st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		return st.DeleteBlob(ctx, bm.BlobID)
	}); err != nil {
		t.Fatalf("unable to clear Azure blob container: %v", err)
	}

	if err := st.Close(ctx); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestAzureStorageInvalidBlob(t *testing.T) {
	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	ctx := context.Background()
	st, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
	})

	if err != nil {
		t.Fatalf("unable to connect to Azure container: %v", err)
	}

	defer st.Close(ctx)

	_, err = st.GetBlob(ctx, "xxx", 0, 30)
	if err == nil {
		t.Errorf("unexpected success when adding to non-existent container")
	}
}

func TestAzureStorageInvalidContainer(t *testing.T) {
	container := fmt.Sprintf("invalid-container-%v", time.Now().UnixNano())
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	ctx := context.Background()
	_, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
	})

	if err == nil {
		t.Errorf("unexpected success connecting to Azure container, wanted error")
	}
}

func TestAzureStorageInvalidCreds(t *testing.T) {
	storageAccount := "invalid-acc"
	storageKey := "invalid-key"
	container := "invalid-container"

	ctx := context.Background()
	_, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
	})

	if err == nil {
		t.Errorf("unexpected success connecting to Azure blob storage, wanted error")
	}
}
