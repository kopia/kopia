package s3

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"log"
	"net"
	"net/http"
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
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
)

const (
	// https://github.com/minio/minio-go
	minioEndpoint        = "play.minio.io:9000"
	minioHost            = "play.minio.io"
	minioAccessKeyID     = "Q3AM3UQ867SPQQA43P2F"
	minioSecretAccessKey = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	minioUseSSL          = true
	minioRegion          = "us-east-1"

	// default aws S3 endpoint.
	awsEndpoint = "s3.amazonaws.com"

	// the test takes a few seconds, delete stuff older than 1h to avoid accumulating cruft.
	cleanupAge = 1 * time.Hour

	// env vars need to be set to execute TestS3StorageAWS.
	testEndpointEnv        = "KOPIA_S3_TEST_ENDPOINT"
	testAccessKeyIDEnv     = "KOPIA_S3_TEST_ACCESS_KEY_ID"
	testSecretAccessKeyEnv = "KOPIA_S3_TEST_SECRET_ACCESS_KEY"
	testBucketEnv          = "KOPIA_S3_TEST_BUCKET"
	testRegionEnv          = "KOPIA_S3_TEST_REGION"
	// additional env vars need to be set to execute TestS3StorageAWSSTS.
	testSTSAccessKeyIDEnv     = "KOPIA_S3_TEST_STS_ACCESS_KEY_ID"
	testSTSSecretAccessKeyEnv = "KOPIA_S3_TEST_STS_SECRET_ACCESS_KEY"
	testSessionTokenEnv       = "KOPIA_S3_TEST_SESSION_TOKEN"

	expiredBadSSL       = "https://expired.badssl.com/"
	selfSignedBadSSL    = "https://self-signed.badssl.com/"
	untrustedRootBadSSL = "https://untrusted-root.badssl.com/"
	wrongHostBadSSL     = "https://wrong.host.badssl.com/"
)

var minioBucketName = getBucketName()

func getBucketName() string {
	hn, err := os.Hostname()
	if err != nil {
		return "kopia-test-1"
	}

	h := sha1.New()
	fmt.Fprintf(h, "%v", hn)

	return fmt.Sprintf("kopia-test-%x", h.Sum(nil)[0:8])
}

func generateName(name string) string {
	b := make([]byte, 3)

	_, err := rand.Read(b)
	if err != nil {
		return fmt.Sprintf("%s-1", name)
	}

	return fmt.Sprintf("%s-%x", name, b)
}

func getEnvOrSkip(t *testutil.RetriableT, name string) string {
	value, ok := os.LookupEnv(name)
	if !ok {
		t.Skip(fmt.Sprintf("Environment variable '%s' not provided", name))
	}

	return value
}

func getEnv(name, defValue string) string {
	value, ok := os.LookupEnv(name)
	if !ok {
		return defValue
	}

	return value
}

func endpointReachable(endpoint string) bool {
	conn, err := net.DialTimeout("tcp4", endpoint, 5*time.Second)
	if err == nil {
		conn.Close()
		return true
	}

	return false
}

func TestS3StorageAWS(t *testing.T) {
	t.Parallel()

	testutil.Retry(t, func(t *testutil.RetriableT) {
		// skip the test if AWS creds are not provided
		options := &Options{
			Endpoint:        getEnv(testEndpointEnv, awsEndpoint),
			AccessKeyID:     getEnvOrSkip(t, testAccessKeyIDEnv),
			SecretAccessKey: getEnvOrSkip(t, testSecretAccessKeyEnv),
			BucketName:      getEnvOrSkip(t, testBucketEnv),
			Region:          getEnvOrSkip(t, testRegionEnv),
		}

		createBucket(t, options)
		testStorage(t, options)
	})
}

func TestS3StorageAWSSTS(t *testing.T) {
	t.Parallel()

	testutil.Retry(t, func(t *testutil.RetriableT) {
		// skip the test if AWS STS creds are not provided
		options := &Options{
			Endpoint:        getEnv(testEndpointEnv, awsEndpoint),
			AccessKeyID:     getEnvOrSkip(t, testSTSAccessKeyIDEnv),
			SecretAccessKey: getEnvOrSkip(t, testSTSSecretAccessKeyEnv),
			SessionToken:    getEnvOrSkip(t, testSessionTokenEnv),
			BucketName:      getEnvOrSkip(t, testBucketEnv),
			Region:          getEnvOrSkip(t, testRegionEnv),
		}

		// STS token may no have permission to create bucket
		// use accesskeyid and secretaccesskey to create the bucket
		createBucket(t, &Options{
			Endpoint:        getEnv(testEndpointEnv, awsEndpoint),
			AccessKeyID:     getEnv(testAccessKeyIDEnv, ""),
			SecretAccessKey: getEnv(testSecretAccessKeyEnv, ""),
			BucketName:      options.BucketName,
			Region:          options.Region,
		})
		testStorage(t, options)
	})
}

func TestS3StorageMinio(t *testing.T) {
	t.Parallel()

	for _, disableTLSVerify := range []bool{true, false} {
		disableTLSVerify := disableTLSVerify

		testutil.Retry(t, func(t *testutil.RetriableT) {
			options := &Options{
				Endpoint:        minioEndpoint,
				AccessKeyID:     minioAccessKeyID,
				SecretAccessKey: minioSecretAccessKey,
				BucketName:      minioBucketName,
				Region:          minioRegion,
				DoNotUseTLS:     !minioUseSSL,
				DoNotVerifyTLS:  disableTLSVerify,
			}

			if !endpointReachable(options.Endpoint) {
				t.Skip("endpoint not reachable")
			}

			createBucket(t, options)
			testStorage(t, options)
		})
	}
}

func TestS3StorageMinioSTS(t *testing.T) {
	t.Parallel()

	for _, disableTLSVerify := range []bool{true, false} {
		disableTLSVerify := disableTLSVerify

		testutil.Retry(t, func(t *testutil.RetriableT) {
			// create kopia user and session token
			kopiaUserName := generateName("kopiauser")
			kopiaUserPasswd := generateName("kopiapassword")

			createMinioUser(t, kopiaUserName, kopiaUserPasswd)
			defer deleteMinioUser(t, kopiaUserName)
			kopiaAccessKeyID, kopiaSecretKey, kopiaSessionToken := createMinioSessionToken(t, kopiaUserName, kopiaUserPasswd, minioBucketName)

			options := &Options{
				Endpoint:        minioEndpoint,
				AccessKeyID:     kopiaAccessKeyID,
				SecretAccessKey: kopiaSecretKey,
				SessionToken:    kopiaSessionToken,
				BucketName:      minioBucketName,
				Region:          minioRegion,
				DoNotUseTLS:     !minioUseSSL,
				DoNotVerifyTLS:  disableTLSVerify,
			}

			if !endpointReachable(options.Endpoint) {
				t.Skip("endpoint not reachable")
			}

			createBucket(t, options)
			testStorage(t, options)
		})
	}
}

func testStorage(t *testutil.RetriableT, options *Options) {
	ctx := context.Background()

	data := make([]byte, 8)
	rand.Read(data)

	cleanupOldData(ctx, t, options)

	options.Prefix = fmt.Sprintf("test-%v-%x-", time.Now().Unix(), data)
	attempt := func() (interface{}, error) {
		return New(testlogging.Context(t), options)
	}

	v, err := retry.WithExponentialBackoff(ctx, "New() S3 storage", attempt, func(err error) bool { return err != nil })
	if err != nil {
		t.Fatalf("err: %v, options:%v", err, options)
	}

	st := v.(blob.Storage)
	blobtesting.VerifyStorage(ctx, t, st)
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)

	if err := st.Close(ctx); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestCustomTransportNoSSLVerify(t *testing.T) {
	testURL(expiredBadSSL, t)
	testURL(selfSignedBadSSL, t)
	testURL(untrustedRootBadSSL, t)
	testURL(wrongHostBadSSL, t)
}

func getURL(url string, insecureSkipVerify bool) error {
	client := &http.Client{Transport: getCustomTransport(insecureSkipVerify)}

	resp, err := client.Get(url) // nolint:noctx
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	return nil
}

func testURL(url string, t *testing.T) {
	err := getURL(url, true)
	if err != nil {
		t.Fatalf("could not get url:%s, error:%v", url, err)
	}

	err = getURL(url, false)
	if err == nil {
		t.Fatalf("expected a TLS issue, but none found for url:%s", url)
	}
}

func createBucket(t *testutil.RetriableT, opt *Options) {
	minioClient, err := minio.New(opt.Endpoint, opt.AccessKeyID, opt.SecretAccessKey, !opt.DoNotUseTLS)
	if err != nil {
		t.Fatalf("can't initialize minio client: %v", err)
	}
	// ignore error
	_ = minioClient.MakeBucket(opt.BucketName, opt.Region)
}

func createMinioUser(t *testutil.RetriableT, kopiaUserName, kopiaPasswd string) {
	// create minio admin client
	adminCli, err := madmin.New(minioEndpoint, minioAccessKeyID, minioSecretAccessKey, minioUseSSL)
	if err != nil {
		t.Fatalf("can't initialize minio admin client: %v", err)
	}

	ctx := testlogging.Context(t)
	// add new kopia user
	if err = adminCli.AddUser(ctx, kopiaUserName, kopiaPasswd); err != nil {
		t.Fatalf("failed to add new minio user: %v", err)
	}

	// set user policy
	if err = adminCli.SetPolicy(ctx, "readwrite", kopiaUserName, false); err != nil {
		t.Fatalf("failed to set user policy: %v", err)
	}
}

func deleteMinioUser(t *testutil.RetriableT, kopiaUserName string) {
	// create minio admin client
	adminCli, err := madmin.New(minioEndpoint, minioAccessKeyID, minioSecretAccessKey, minioUseSSL)
	if err != nil {
		t.Fatalf("can't initialize minio admin client: %v", err)
	}

	// delete temp kopia user
	// ignore error
	_ = adminCli.RemoveUser(testlogging.Context(t), kopiaUserName)
}

func createMinioSessionToken(t *testutil.RetriableT, kopiaUserName, kopiaUserPasswd, bucketName string) (accessID, secretKey, sessionToken string) {
	// Configure to use MinIO Server
	awsConfig := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(kopiaUserName, kopiaUserPasswd, ""),
		Endpoint:         aws.String(minioHost),
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

func cleanupOldData(ctx context.Context, t *testutil.RetriableT, options *Options) {
	// cleanup old data from the bucket
	st, err := New(testlogging.Context(t), options)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	_ = st.ListBlobs(ctx, "", func(it blob.Metadata) error {
		age := time.Since(it.Timestamp)
		if age > cleanupAge {
			if err := st.DeleteBlob(ctx, it.BlobID); err != nil {
				t.Errorf("warning: unable to delete %q: %v", it.BlobID, err)
			}
		}
		return nil
	})
}
