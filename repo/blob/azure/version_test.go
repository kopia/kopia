package azure

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/format"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
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
)

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

func putBlobs(ctx context.Context, cli blob.Storage, blobID blob.ID, blobs []string) ([]time.Time, error) {
	var putTimes []time.Time

	for _, b := range blobs {
		if err := cli.PutBlob(ctx, blobID, gather.FromSlice([]byte(b)), blob.PutOptions{}); err != nil {
			return nil, errors.Wrap(err, "putting blob")
		}

		m, err := cli.GetMetadata(ctx, blobID)
		if err != nil {
			return nil, errors.Wrap(err, "getting metadata")
		}

		putTimes = append(putTimes, m.Timestamp)
		// sleep because granularity is 1 second and we should separate to show PIT views.
		time.Sleep(1 * time.Second)
	}

	return putTimes, nil
}

func getEnvOrSkip(t *testing.T, name string) string {
	t.Helper()

	value := os.Getenv(name)
	if value == "" {
		t.Skipf("%s not provided", name)
	}

	return value
}

func storagePrefixForTest() string {
	var data [4]byte

	rand.Read(data[:])

	return fmt.Sprintf("test-%v-%x/", clock.Now().Unix(), data)
}

func cleanupTestDataAndClose(tb testing.TB, st blob.Storage) {
	tb.Helper()

	ctx := testlogging.ContextForCleanup(tb)

	blobtesting.CleanupOldData(ctx, tb, st, 0)

	require.NoError(tb, st.Close(ctx))
}

func TestBlobVersionsSequence(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	// must be with Immutable Storage with Versioning enabled
	container := getEnvOrSkip(t, testImmutableContainerEnv)
	storageAccount := getEnvOrSkip(t, testImmutableStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testImmutableStorageKeyEnv)

	createContainer(t, container, storageAccount, storageKey)

	ctx := testlogging.Context(t)
	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)

	opts := &Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
		Prefix:         storagePrefixForTest(),
	}
	st, err := New(newctx, opts, false)
	require.NoError(t, err)

	cancel()

	// required for PIT versioning check
	err = st.PutBlob(ctx, format.KopiaRepositoryBlobID, gather.FromSlice([]byte(nil)), blob.PutOptions{})
	require.NoError(t, err)
	require.NoError(t, st.Close(ctx))

	now := time.Now().Add(time.Second) // ensure kopia.repository blob is included
	opts.PointInTime = &now
	newctx, cancel = context.WithCancel(ctx)
	st, err = New(newctx, opts, false)

	require.NoError(t, err)
	cancel()

	t.Logf("pitSt: %T", st)

	pitSt, isPIT := st.(*azPointInTimeStorage)
	require.True(t, isPIT, "is not point-in-time store")

	cSt := st // grab reference to prevent clobbering that results in SIGSEV during t.Cleanup

	t.Cleanup(func() { cleanupTestDataAndClose(t, cSt) })

	const (
		originalData = "original"
		updatedData  = "some update"
		latestData   = "latest version"
	)

	blobID := blob.ID(t.Name())
	dataBlobs := []string{originalData, updatedData}
	dataTimestamps, err := putBlobs(ctx, st, blobID, dataBlobs)

	require.NoError(t, err)
	require.Len(t, dataTimestamps, 2)

	listBlobVersions(ctx, t, blobID, pitSt)

	t.Log("--- delete blob")
	time.Sleep(2 * time.Second)

	err = st.DeleteBlob(ctx, blobID)
	require.NoError(t, err)

	listBlobVersions(ctx, t, blobID, pitSt)

	t.Log("--- add blob version")

	dataTimestamps2, err := putBlobs(ctx, st, blobID, []string{latestData})

	require.NoError(t, err)
	require.Len(t, dataTimestamps2, 1)
	listBlobVersions(ctx, t, blobID, pitSt)

	t.Log("--- delete blob")
	time.Sleep(2 * time.Second)

	err = st.DeleteBlob(ctx, blobID)
	require.NoError(t, err)

	vms := listBlobVersions(ctx, t, blobID, pitSt)
	require.Len(t, vms, 4)

	bc, err := pitSt.service.ServiceClient().
		NewContainerClient(pitSt.container).
		NewBlobClient(pitSt.getObjectNameString(blobID)).
		WithVersionID(vms[2].Version)
	require.NoError(t, err)

	_, err = bc.Delete(ctx, nil)
	require.NoError(t, err)

	listBlobVersions(ctx, t, blobID, pitSt)
}

func listBlobVersions(ctx context.Context, tb testing.TB, prefix blob.ID, pit *azPointInTimeStorage) []versionMetadata {
	tb.Helper()

	var vms []versionMetadata

	pit.listBlobVersions(ctx, prefix, func(vm versionMetadata) error {
		vms = append(vms, vm)
		tb.Logf("blob version: %v | %v | %q", vm.IsDeleteMarker, vm.Timestamp, vm.Version)
		return nil
	})

	return vms
}
