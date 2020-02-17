package s3

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
	minio "github.com/minio/minio-go/v6"
	"github.com/minio/minio/pkg/madmin"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
)

// https://github.com/minio/minio-go
const (
	endpoint        = "play.minio.io:9000"
	host            = "play.minio.io"
	accessKeyID     = "Q3AM3UQ867SPQQA43P2F"                     //nolint:gosec
	secretAccessKey = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG" //nolint:gosec
	useSSL          = true
	kopiaUserName   = "kopiauser"
	kopiaUserPasswd = "kopia@1234"

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

func endpointReachable() bool {
	conn, err := net.DialTimeout("tcp4", endpoint, 5*time.Second)
	if err == nil {
		conn.Close()
		return true
	}

	return false
}

func TestS3Storage(t *testing.T) {
	testStorage(t, accessKeyID, secretAccessKey, "")
}

func TestS3StorageWithSessionToken(t *testing.T) {
	// create kopia user and session token
	createUser(t)
	kopiaAccessKeyID, kopiaSecretKey, kopiaSessionToken := createTemporaryCreds(t)
	testStorage(t, kopiaAccessKeyID, kopiaSecretKey, kopiaSessionToken)
}

func testStorage(t *testing.T, accessID, secretKey, sessionToken string) {
	if !endpointReachable() {
		t.Skip("endpoint not reachable")
	}

	ctx := context.Background()

	// recreate per-host bucket, which sometimes get cleaned up by play.minio.io
	createBucket(t)
	cleanupOldData(ctx, t)

	data := make([]byte, 8)
	rand.Read(data) //nolint:errcheck

	attempt := func() (interface{}, error) {
		return New(context.Background(), &Options{
			AccessKeyID:     accessID,
			SecretAccessKey: secretKey,
			SessionToken:    sessionToken,
			Endpoint:        endpoint,
			BucketName:      bucketName,
			Prefix:          fmt.Sprintf("test-%v-%x-", time.Now().Unix(), data),
		})
	}

	v, err := retry.WithExponentialBackoff("New() S3 storage", attempt, func(err error) bool { return err != nil })
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	st := v.(blob.Storage)
	blobtesting.VerifyStorage(ctx, t, st)
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)

	if err := st.Close(ctx); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func createBucket(t *testing.T) {
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		t.Fatalf("can't initialize minio client: %v", err)
	}
	// ignore error
	_ = minioClient.MakeBucket(bucketName, "us-east-1")
}

func createUser(t *testing.T) {
	// create minio admin
	adminCli, err := madmin.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		t.Fatalf("can't initialize minio admin client: %v", err)
	}

	// add new kopia user
	if err = adminCli.AddUser(kopiaUserName, kopiaUserPasswd); err != nil {
		t.Fatalf("failed to add new minio user: %v", err)
	}

	// set user policy
	if err = adminCli.SetPolicy("readwrite", kopiaUserName, false); err != nil {
		t.Fatalf("failed to set user policy: %v", err)
	}
}

func createTemporaryCreds(t *testing.T) (accessID, secretKey, sessionToken string) {
	// Configure to use MinIO Server
	awsConfig := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(kopiaUserName, kopiaUserPasswd, ""),
		Endpoint:         aws.String(host),
		Region:           aws.String("us-east-1"),
		S3ForcePathStyle: aws.Bool(true),
	}

	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		t.Fatalf("failed to create aws session: %v", err)
	}

	svc := sts.New(awsSession)

	input := &sts.AssumeRoleInput{
		// give access to only S3 bucket with name bucketName
		Policy: aws.String(fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Sid":"Stmt1","Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::%s/*"}]}`, bucketName)),
		// RoleArn and RoleSessionName are not meaningful for MinIO and can be set to any value
		RoleArn:         aws.String("arn:xxx:xxx:xxx:xxxx"),
		RoleSessionName: aws.String("kopiaTestSession"),
		DurationSeconds: aws.Int64(900), // in seconds
	}

	result, err := svc.AssumeRole(input)
	if err != nil {
		t.Fatalf("failed to create session with aws assume role: %v", err)
	}

	if result.Credentials == nil {
		t.Fatalf("couldn't find aws creds in aws assume role response")
	}

	log.Printf("created session token with assume role: expiration: %s", result.Credentials.Expiration)

	return *result.Credentials.AccessKeyId, *result.Credentials.SecretAccessKey, *result.Credentials.SessionToken
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

	_ = st.ListBlobs(ctx, "", func(it blob.Metadata) error {
		age := time.Since(it.Timestamp)
		if age > cleanupAge {
			if err := st.DeleteBlob(ctx, it.BlobID); err != nil {
				t.Errorf("warning: unable to delete %q: %v", it.BlobID, err)
			}
		} else {
			log.Printf("keeping %v", it.BlobID)
		}
		return nil
	})
}
