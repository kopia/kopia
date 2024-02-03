package azure_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/azure"
	"github.com/kopia/kopia/repo/content"
)

// TestAzureStorageImmutabilityProtection runs through the behavior of Azure immutability protection.
func TestAzureStorageImmutabilityProtection(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	// must be with ImmutableStorage with Versioning enabled
	container := getEnvOrSkip(t, testImmutableContainerEnv)
	storageAccount := getEnvOrSkip(t, testImmutableStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testImmutableStorageKeyEnv)

	data := make([]byte, 8)
	rand.Read(data)

	ctx := testlogging.Context(t)

	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	prefix := fmt.Sprintf("test-%v-%x/", clock.Now().Unix(), data)
	st, err := azure.New(newctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
		Prefix:         prefix,
	}, false)

	cancel()
	require.NoError(t, err)

	t.Cleanup(func() {
		st.Close(ctx)
	})

	const (
		blobName  = "sExample"
		dummyBlob = blob.ID(blobName)
	)

	blobNameFullPath := prefix + blobName

	putOpts := blob.PutOptions{
		RetentionMode:   blob.Compliance,
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
	// this has potential to flake if Azure is too slow; RetentionPeriod may need to be increased to more than 3 seconds if so
	if !blobRetention.After(currentTime) {
		t.Fatalf("blob retention period not in the future: %v", blobRetention)
	}

	extendOpts := blob.ExtendOptions{
		RetentionMode:   blob.Compliance,
		RetentionPeriod: 10 * time.Second,
	}
	err = st.ExtendBlobRetention(ctx, dummyBlob, extendOpts)
	require.NoError(t, err)

	extendedRetention := getBlobRetention(ctx, t, cli, container, blobNameFullPath)
	if !extendedRetention.After(blobRetention) {
		t.Fatalf("blob retention period not extended. was %v, now %v", blobRetention, extendedRetention)
	}

	// DeleteImmutabilityPolicy fails on a locked policy
	_, err = cli.ServiceClient().NewContainerClient(container).NewBlobClient(prefix+string(dummyBlob)).DeleteImmutabilityPolicy(ctx, nil)
	require.Error(t, err)

	var re *azcore.ResponseError

	require.ErrorAs(t, err, &re)
	require.Equal(t, "ImmutabilityPolicyDeleteOnLockedPolicy", re.ErrorCode)

	err = st.DeleteBlob(ctx, dummyBlob)
	require.NoError(t, err)

	count = getBlobCount(ctx, t, st, content.BlobIDPrefixSession)
	require.Equal(t, 0, count)
}

func getBlobRetention(ctx context.Context, t *testing.T, cli *azblob.Client, container, blobName string) time.Time {
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
