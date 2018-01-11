package s3

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/storagetesting"
)

// https://github.com/minio/minio-go
const (
	endpoint        = "play.minio.io:9000"
	accessKeyID     = "Q3AM3UQ867SPQQA43P2F"
	secretAccessKey = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	useSSL          = true

	// the test takes a few seconds, delete stuff older than 1h to avoid accumulating cruft
	cleanupAge = 1 * time.Hour

	bucketName = "kopia-test-1"
)

func TestS3Storage(t *testing.T) {
	if testing.Short() {
		return
	}

	cleanupOldData(t)

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

	storagetesting.VerifyStorage(t, st)
}

func cleanupOldData(t *testing.T) {
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

	items, cancel := st.ListBlocks("")
	defer cancel()
	for it := range items {
		if it.Error != nil {
			t.Errorf("can't cleanup: %v", it.Error)
			return
		}

		age := time.Since(it.TimeStamp)
		if age > cleanupAge {
			if err := st.DeleteBlock(it.BlockID); err != nil {
				t.Errorf("warning: unable to delete %q: %v", it.BlockID, err)
			}
		} else {
			log.Printf("keeping %v", it.BlockID)
		}
	}
}
