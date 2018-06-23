package s3

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/storagetesting"
	"github.com/kopia/kopia/storage"
	"github.com/minio/minio-go"
)

// https://github.com/minio/minio-go
const (
	endpoint        = "play.minio.io:9000"
	accessKeyID     = "Q3AM3UQ867SPQQA43P2F"
	secretAccessKey = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	useSSL          = true

	// the test takes a few seconds, delete stuff older than 1h to avoid accumulating cruft
	cleanupAge = 1 * time.Hour
)

var bucketName = getBucketName()

func getBucketName() string {
	hn, err := os.Hostname()
	if err != nil {
		return "kopia-test-1"
	}
	h := sha1.New()
	fmt.Fprintf(h, "%v", hn)
	return fmt.Sprintf("kopia-test-%x", h.Sum(nil)[0:8])
}

func TestS3Storage(t *testing.T) {
	if testing.Short() {
		return
	}

	ctx := context.Background()

	// recreate per-host bucket, which sometimes get cleaned up by play.minio.io
	createBucket(t)
	cleanupOldData(ctx, t)

	data := make([]byte, 8)
	rand.Read(data)

	st, err := New(context.Background(), &Options{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		Endpoint:        endpoint,
		BucketName:      bucketName,
		Prefix:          fmt.Sprintf("test-%v-%x-", time.Now().Unix(), data),
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	storagetesting.VerifyStorage(ctx, t, st)
}

func createBucket(t *testing.T) {
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		t.Fatalf("can't initialize minio client: %v", err)
	}
	minioClient.MakeBucket(bucketName, "us-east-1")
}

func cleanupOldData(ctx context.Context, t *testing.T) {
	// cleanup old data from the bucket
	st, err := New(context.Background(), &Options{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		Endpoint:        endpoint,
		BucketName:      bucketName,
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	st.ListBlocks(ctx, "", func(it storage.BlockMetadata) error {
		age := time.Since(it.Timestamp)
		if age > cleanupAge {
			if err := st.DeleteBlock(ctx, it.BlockID); err != nil {
				t.Errorf("warning: unable to delete %q: %v", it.BlockID, err)
			}
		} else {
			log.Printf("keeping %v", it.BlockID)
		}
		return nil
	})
}
