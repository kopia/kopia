package azure_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	legacyazblob "github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/azure"
	"github.com/kopia/kopia/repo/content"
)

const (
	testContainerEnv           = "KOPIA_AZURE_TEST_CONTAINER"
	testStorageAccountEnv      = "KOPIA_AZURE_TEST_STORAGE_ACCOUNT"
	testStorageKeyEnv          = "KOPIA_AZURE_TEST_STORAGE_KEY"
	testStorageSASTokenEnv     = "KOPIA_AZURE_TEST_SAS_TOKEN"
	testStorageTenantIDEnv     = "KOPIA_AZURE_TEST_TENANT_ID"
	testStorageClientIDEnv     = "KOPIA_AZURE_TEST_CLIENT_ID"
	testStorageClientSecretEnv = "KOPIA_AZURE_TEST_CLIENT_SECRET"
)

func getEnvOrSkip(t *testing.T, name string) string {
	t.Helper()

	value := os.Getenv(name)
	if value == "" {
		t.Skipf("%s not provided", name)
	}

	return value
}

func createContainer(t *testing.T, container, storageAccount, storageKey string) {
	t.Helper()

	credential, err := legacyazblob.NewSharedKeyCredential(storageAccount, storageKey)
	if err != nil {
		t.Fatalf("failed to create Azure credentials: %v", err)
	}

	p := legacyazblob.NewPipeline(credential, legacyazblob.PipelineOptions{})

	u, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", storageAccount))
	if err != nil {
		t.Fatalf("failed to parse container URL: %v", err)
	}

	serviceURL := legacyazblob.NewServiceURL(*u, p)
	containerURL := serviceURL.NewContainerURL(container)

	_, err = containerURL.Create(context.Background(), legacyazblob.Metadata{}, legacyazblob.PublicAccessNone)
	if err == nil {
		return
	}

	// return if already exists
	var stgErr legacyazblob.StorageError
	if errors.As(err, &stgErr) {
		if stgErr.ServiceCode() == legacyazblob.ServiceCodeContainerAlreadyExists {
			return
		}
	}

	t.Fatalf("failed to create blob storage container: %v", err)
}

func TestCleanupOldData(t *testing.T) {
	ctx := testlogging.Context(t)

	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	st, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
	}, false)

	require.NoError(t, err)

	defer st.Close(ctx)

	blobtesting.CleanupOldData(ctx, t, st, blobtesting.MinCleanupAge)
}

func TestAzureStorage(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	// create container if does not exist
	createContainer(t, container, storageAccount, storageKey)

	data := make([]byte, 8)
	rand.Read(data)

	ctx := testlogging.Context(t)

	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	st, err := azure.New(newctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
		Prefix:         fmt.Sprintf("test-%v-%x-", clock.Now().Unix(), data),
	}, false)

	cancel()
	require.NoError(t, err)

	defer st.Close(ctx)

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

func TestAzureStorageSASToken(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	sasToken := getEnvOrSkip(t, testStorageSASTokenEnv)

	data := make([]byte, 8)
	rand.Read(data)

	ctx := testlogging.Context(t)

	// use context that gets canceled after storage is initialize,
	// to verify we do not depend on the original context past initialization.
	newctx, cancel := context.WithCancel(ctx)
	st, err := azure.New(newctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		SASToken:       sasToken,
		Prefix:         fmt.Sprintf("sastest-%v-%x-", clock.Now().Unix(), data),
	}, false)

	require.NoError(t, err)
	cancel()

	defer st.Close(ctx)
	defer blobtesting.CleanupOldData(ctx, t, st, 0)

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

func TestAzureStorageClientSecret(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	tenantID := getEnvOrSkip(t, testStorageTenantIDEnv)
	clientID := getEnvOrSkip(t, testStorageClientIDEnv)
	clientSecret := getEnvOrSkip(t, testStorageClientSecretEnv)

	data := make([]byte, 8)
	rand.Read(data)

	ctx := testlogging.Context(t)

	// use context that gets canceled after storage is initialize,
	// to verify we do not depend on the original context past initialization.
	newctx, cancel := context.WithCancel(ctx)
	st, err := azure.New(newctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		TenantID:       tenantID,
		ClientID:       clientID,
		ClientSecret:   clientSecret,
		Prefix:         fmt.Sprintf("sastest-%v-%x-", clock.Now().Unix(), data),
	}, false)

	require.NoError(t, err)
	cancel()

	defer st.Close(ctx)
	defer blobtesting.CleanupOldData(ctx, t, st, 0)

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

func TestAzureStorageInvalidBlob(t *testing.T) {
	testutil.ProviderTest(t)

	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	ctx := context.Background()

	st, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
	}, false)
	if err != nil {
		t.Fatalf("unable to connect to Azure container: %v", err)
	}

	defer st.Close(ctx)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	err = st.GetBlob(ctx, "xxx", 0, 30, &tmp)
	if err == nil {
		t.Errorf("unexpected success when adding to non-existent container")
	}
}

func TestAzureStorageInvalidContainer(t *testing.T) {
	testutil.ProviderTest(t)

	container := fmt.Sprintf("invalid-container-%v", clock.Now().UnixNano())
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	ctx := context.Background()
	_, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
	}, false)

	if err == nil {
		t.Errorf("unexpected success connecting to Azure container, wanted error")
	}
}

func TestAzureStorageInvalidCreds(t *testing.T) {
	testutil.ProviderTest(t)

	storageAccount := "invalid-acc"
	storageKey := "invalid-key"
	container := "invalid-container"

	ctx := context.Background()
	_, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
	}, false)

	if err == nil {
		t.Errorf("unexpected success connecting to Azure blob storage, wanted error")
	}
}

// TestAzureStorageImmutabilityProtection runs through the behavior of Azure immutability protection.
func TestAzureStorageImmutabilityProtection(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	// must be with ImmutableStorage with Versioning enabled
	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	// create container if does not exist
	createContainer(t, container, storageAccount, storageKey)

	data := make([]byte, 8)
	rand.Read(data)

	ctx := testlogging.Context(t)

	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	prefix := fmt.Sprintf("test-%v-%x-", clock.Now().Unix(), data)
	st, err := azure.New(newctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
		Prefix:         prefix,
	}, false)

	cancel()
	require.NoError(t, err)

	defer st.Close(ctx)

	const (
		blobName  = "sExample"
		dummyBlob = blob.ID(blobName)
	)

	blobNameFullPath := prefix + blobName

	putOpts := blob.PutOptions{
		RetentionMode:   blob.Locked,
		RetentionPeriod: 3 * time.Second,
	}
	// non-nil blob to distinguish against delete marker version
	err = st.PutBlob(ctx, dummyBlob, gather.FromSlice([]byte("x")), putOpts)
	require.NoError(t, err)
	cli := getAzureCLI(t, storageAccount, storageKey)

	count := getBlobCount(ctx, t, st, content.BlobIDPrefixSession)
	require.Equal(t, 1, count)

	currentTime := clock.Now().UTC()

	blobRetention := getBlobRetention(ctx, t, cli, container, blobNameFullPath)
	if !blobRetention.After(currentTime) {
		t.Fatalf("blob retention period not in the future: %v", blobRetention)
		t.FailNow()
	}

	extendOpts := blob.ExtendOptions{
		RetentionMode:   blob.Locked,
		RetentionPeriod: 4 * time.Second,
	}
	err = st.ExtendBlobRetention(ctx, dummyBlob, extendOpts)
	require.NoError(t, err)

	extendedRetention := getBlobRetention(ctx, t, cli, container, blobNameFullPath)
	if !extendedRetention.After(blobRetention) {
		t.Fatalf("blob retention period not extended. was %v, now %v", blobRetention, extendedRetention)
		t.FailNow()
	}

	err = st.DeleteBlob(ctx, dummyBlob)
	require.NoError(t, err)

	count = getBlobCount(ctx, t, st, content.BlobIDPrefixSession)
	require.Equal(t, 0, count)
}

func getBlobCount(ctx context.Context, t *testing.T, st blob.Storage, prefix blob.ID) int {
	t.Helper()

	var count int

	err := st.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
		count++
		return nil
	})
	require.NoError(t, err)

	return count
}

func getBlobRetention(ctx context.Context, t *testing.T, cli *azblob.Client, container string, blobName string) time.Time {
	t.Helper()

	props, err := cli.ServiceClient().
		NewContainerClient(container).
		NewBlobClient(blobName).
		GetProperties(ctx, nil)
	require.NoError(t, err)

	return *props.ImmutabilityPolicyExpiresOn
}

// getAzureCLI returns a separate client to verify things the Storage interface doesn't support.
func getAzureCLI(t *testing.T, storageAccount, storageKey string) *azblob.Client {
	t.Helper()

	cred, err := azblob.NewSharedKeyCredential(storageAccount, storageKey)
	require.NoError(t, err)

	storageHostname := fmt.Sprintf("%v.blob.core.windows.net", storageAccount)
	cli, err := azblob.NewClientWithSharedKeyCredential(
		fmt.Sprintf("https://%s/", storageHostname), cred, nil,
	)
	require.NoError(t, err)

	return cli
}
