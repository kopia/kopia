package azure_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/azure"
)

const (
	testContainerEnv                      = "KOPIA_AZURE_TEST_CONTAINER"
	testStorageAccountEnv                 = "KOPIA_AZURE_TEST_STORAGE_ACCOUNT"
	testStorageKeyEnv                     = "KOPIA_AZURE_TEST_STORAGE_KEY"
	testStorageSASTokenEnv                = "KOPIA_AZURE_TEST_SAS_TOKEN"
	testImmutableContainerEnv             = "KOPIA_AZURE_TEST_IMMUTABLE_CONTAINER"
	testImmutableStorageAccountEnv        = "KOPIA_AZURE_TEST_IMMUTABLE_STORAGE_ACCOUNT"
	testImmutableStorageKeyEnv            = "KOPIA_AZURE_TEST_IMMUTABLE_STORAGE_KEY"
	testStorageTenantIDEnv                = "KOPIA_AZURE_TEST_TENANT_ID"
	testStorageClientIDEnv                = "KOPIA_AZURE_TEST_CLIENT_ID"
	testStorageClientSecretEnv            = "KOPIA_AZURE_TEST_CLIENT_SECRET"
	testStorageClientCertEnv              = "KOPIA_AZURE_TEST_CLIENT_CERTIFICATE"
	testAzureFederatedIdentityFilePathEnv = "KOPIA_AZURE_FEDERATED_IDENTITY_FILE_PATH"
	// Test env to trigger Azure CLI credential usage in integration tests.
	testUseAzureCLICredEnv = "KOPIA_AZURE_USE_CLI_CREDENTIAL"
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

	credential, err := azblob.NewSharedKeyCredential(storageAccount, storageKey)
	require.NoError(t, err, "failed to create Azure credentials")

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net", storageAccount)

	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, credential, nil)
	require.NoError(t, err, "failed to get azblob client")

	_, err = client.CreateContainer(context.Background(), container, nil)
	if err == nil {
		return
	}

	// return if already exists
	if bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		return
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

	t.Cleanup(func() {
		st.Close(testlogging.ContextForCleanup(t))
	})

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
		Prefix:         fmt.Sprintf("test-%v-%x/", clock.Now().Unix(), data),
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
		Prefix:         fmt.Sprintf("sastest-%v-%x/", clock.Now().Unix(), data),
	}, false)

	require.NoError(t, err)
	cancel()

	t.Cleanup(func() {
		ctx := testlogging.ContextForCleanup(t)

		blobtesting.CleanupOldData(ctx, t, st, 0)
		st.Close(ctx)
	})

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
		Prefix:         fmt.Sprintf("sastest-%v-%x/", clock.Now().Unix(), data),
	}, false)

	require.NoError(t, err)
	cancel()

	t.Cleanup(func() {
		ctx := testlogging.ContextForCleanup(t)

		blobtesting.CleanupOldData(ctx, t, st, 0)
		st.Close(ctx)
	})

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

func TestAzureStorageClientCertificate(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	tenantID := getEnvOrSkip(t, testStorageTenantIDEnv)
	clientID := getEnvOrSkip(t, testStorageClientIDEnv)
	clientCert := getEnvOrSkip(t, testStorageClientCertEnv)

	data := make([]byte, 8)
	rand.Read(data)

	ctx := testlogging.Context(t)

	// use context that gets canceled after storage is initialize,
	// to verify we do not depend on the original context past initialization.
	newctx, cancel := context.WithCancel(ctx)
	st, err := azure.New(newctx, &azure.Options{
		Container:         container,
		StorageAccount:    storageAccount,
		TenantID:          tenantID,
		ClientID:          clientID,
		ClientCertificate: clientCert,
		Prefix:            fmt.Sprintf("sastest-%v-%x/", clock.Now().Unix(), data),
	}, false)

	require.NoError(t, err)
	cancel()

	t.Cleanup(func() {
		ctx := testlogging.ContextForCleanup(t)

		blobtesting.CleanupOldData(ctx, t, st, 0)
		st.Close(ctx)
	})

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

func TestAzureFederatedIdentity(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	tenantID := getEnvOrSkip(t, testStorageTenantIDEnv)
	clientID := getEnvOrSkip(t, testStorageClientIDEnv)
	azureFederatedTokenFilePath := getEnvOrSkip(t, testAzureFederatedIdentityFilePathEnv)

	data := make([]byte, 8)
	rand.Read(data)

	ctx := testlogging.Context(t)

	// use context that gets canceled after storage is initialize,
	// to verify we do not depend on the original context past initialization.
	newctx, cancel := context.WithCancel(ctx)
	st, err := azure.New(newctx, &azure.Options{
		Container:               container,
		StorageAccount:          storageAccount,
		TenantID:                tenantID,
		ClientID:                clientID,
		AzureFederatedTokenFile: azureFederatedTokenFilePath,
		Prefix:                  fmt.Sprintf("sastest-%v-%x/", clock.Now().Unix(), data),
	}, false)

	require.NoError(t, err)
	cancel()

	t.Cleanup(func() {
		ctx := testlogging.ContextForCleanup(t)

		blobtesting.CleanupOldData(ctx, t, st, 0)
		st.Close(ctx)
	})

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

// TestAzureCLICredential verifies that Kopia can connect to Azure storage using
// Azure CLI cached credential (the same cache filled by `az login` and `az login --identity`).
// Notes:
//   - Ensure the Azure principal used (CLI user or managed identity) has proper RBAC,
//     e.g. Storage Blob Data Contributor on the target account/container.
//   - If testing the CLI credential flow on a VM with managed identity, run:
//     az login --identity
//     so the Azure CLI cache is populated.
//   - Tests perform cleanup but run them against a dedicated test container/account
//     to avoid interfering with production data.
func TestAzureCLICredential(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	// Skip if the specific env enabling CLI credential tests is not set.
	if os.Getenv(testUseAzureCLICredEnv) == "" {
		t.Skipf("%s not set", testUseAzureCLICredEnv)
	}

	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)

	data := make([]byte, 8)
	rand.Read(data)

	ctx := testlogging.Context(t)

	// use context that gets canceled after storage is initialize,
	// to verify we do not depend on the original context past initialization.
	newctx, cancel := context.WithCancel(ctx)
	st, err := azure.New(newctx, &azure.Options{
		Container:             container,
		StorageAccount:        storageAccount,
		UseAzureCLICredential: true, // force using Azure CLI cached credential in the Options
		Prefix:                fmt.Sprintf("clicred-%v-%x/", clock.Now().Unix(), data),
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

	ctx := testlogging.Context(t)

	st, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
	}, false)

	require.NoError(t, err, "unable to connect to Azure container")

	defer st.Close(ctx)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	err = st.GetBlob(ctx, "xxx", 0, 30, &tmp)
	require.Error(t, err, "unexpected success when adding to non-existent container")
}

func TestAzureStorageInvalidContainer(t *testing.T) {
	testutil.ProviderTest(t)

	container := fmt.Sprintf("invalid-container-%v", clock.Now().UnixNano())
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	ctx := testlogging.Context(t)

	_, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
	}, false)

	require.Error(t, err, "unexpected success connecting to Azure container, expected error")
}

func TestAzureStorageInvalidCreds(t *testing.T) {
	testutil.ProviderTest(t)

	storageAccount := "invalid-acc"
	storageKey := "invalid-key"
	container := "invalid-container"

	ctx := testlogging.Context(t)

	_, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
	}, false)

	require.Error(t, err, "unexpected success connecting to Azure blob storage, expected error")
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
