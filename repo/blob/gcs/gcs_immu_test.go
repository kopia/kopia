package gcs_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	gcsclient "cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/gcs"
)

// TestGoogleStorageImmutabilityProtection runs through the behavior of Google immutability protection.
func TestGoogleStorageImmutabilityProtection(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	opts := bucketOpts{
		projectID:       getEnvVarOrSkip(t, testBucketProjectID),
		bucket:          getImmutableBucketNameOrSkip(t),
		credentialsJSON: getCredJSONFromEnv(t),
		isLockedBucket:  true,
	}
	createBucket(t, opts)
	validateBucket(t, opts)

	data := make([]byte, 8)
	rand.Read(data)

	ctx := testlogging.Context(t)

	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	prefix := fmt.Sprintf("test-%v-%x/", clock.Now().Unix(), data)
	st, err := gcs.New(newctx, &gcs.Options{
		BucketName:                   opts.bucket,
		ServiceAccountCredentialJSON: opts.credentialsJSON,
		Prefix:                       prefix,
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
		RetentionPeriod: 3 * time.Second,
	}
	err = st.PutBlob(ctx, dummyBlob, gather.FromSlice([]byte("x")), putOpts)
	require.NoError(t, err)

	count := getBlobCount(ctx, t, st, dummyBlob[:1])
	require.Equal(t, 1, count)

	cli := getGoogleCLI(t, opts.credentialsJSON)

	attrs, err := cli.Bucket(opts.bucket).Object(blobNameFullPath).Attrs(ctx)
	require.NoError(t, err)

	blobRetention := attrs.RetentionExpirationTime
	if !blobRetention.After(attrs.Created) {
		t.Fatalf("blob retention period not in the future enough: %v (created at %v)", blobRetention, attrs.Created)
	}

	extendOpts := blob.ExtendOptions{
		RetentionPeriod: 10 * time.Second,
	}
	err = st.ExtendBlobRetention(ctx, dummyBlob, extendOpts)
	require.NoError(t, err)

	attrs, err = cli.Bucket(opts.bucket).Object(blobNameFullPath).Attrs(ctx)
	require.NoError(t, err)

	extendedRetention := attrs.RetentionExpirationTime
	if !extendedRetention.After(blobRetention) {
		t.Fatalf("blob retention period not extended. was %v, now %v", blobRetention, extendedRetention)
	}

	updAttrs := gcsclient.ObjectAttrsToUpdate{
		Retention: &gcsclient.ObjectRetention{
			Mode:        "Unlocked",
			RetainUntil: clock.Now().Add(10 * time.Minute),
		},
	}
	_, err = cli.Bucket(opts.bucket).Object(blobNameFullPath).OverrideUnlockedRetention(true).Update(ctx, updAttrs)
	require.Error(t, err)
	require.ErrorContains(t, err, "Its retention mode cannot be changed and its retention period cannot be shortened.")

	err = st.DeleteBlob(ctx, dummyBlob)
	require.NoError(t, err)

	count = getBlobCount(ctx, t, st, dummyBlob[:1])
	require.Equal(t, 0, count)
}

// getGoogleCLI returns a separate client to verify things the Storage interface doesn't support.
func getGoogleCLI(t *testing.T, credentialsJSON []byte) *gcsclient.Client {
	t.Helper()

	ctx := context.Background()
	cli, err := gcsclient.NewClient(ctx, option.WithCredentialsJSON(credentialsJSON))

	require.NoError(t, err, "unable to create GCS client")

	return cli
}
