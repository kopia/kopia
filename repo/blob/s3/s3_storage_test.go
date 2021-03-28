package s3

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/google/uuid"
	minio "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/pkg/madmin"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
)

const (
	// https://github.com/minio/minio-go

	// fake creadentials used by minio server we're launching.
	minioAccessKeyID     = "fake-key"
	minioSecretAccessKey = "fake-secret"
	minioUseSSL          = false
	minioRegion          = "fake-region-1"
	minioBucketName      = "my-bucket" // we use ephemeral minio for each test so this does not need to be unique

	// default aws S3 endpoint.
	awsEndpoint = "s3.amazonaws.com"

	// the test takes a few seconds, delete stuff older than 1h to avoid accumulating cruft.
	defaultCleanupAge = 1 * time.Hour

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

var providerCreds = map[string]string{
	"S3":               "KOPIA_S3_CREDS",
	"Wasabi":           "KOPIA_S3_WASABI_CREDS",
	"Wasabi-Versioned": "KOPIA_S3_WASABI_VERSIONED_CREDS",
}

// startDockerMinioOrSkip starts ephemeral minio instance on a random port and returns the endpoint ("localhost:xxx").
func startDockerMinioOrSkip(t *testing.T) string {
	t.Helper()

	testutil.TestSkipOnCIUnlessLinuxAMD64(t)

	containerID := testutil.RunContainerAndKillOnCloseOrSkip(t,
		"run", "--rm", "-p", "0:9000",
		"-e", "MINIO_ROOT_USER="+minioAccessKeyID,
		"-e", "MINIO_ROOT_PASSWORD="+minioSecretAccessKey,
		"-e", "MINIO_REGION_NAME="+minioRegion,
		"-d", "minio/minio", "server", "/data")
	endpoint := testutil.GetContainerMappedPortAddress(t, containerID, "9000")

	t.Logf("endpoint: %v", endpoint)

	return endpoint
}

func generateName(name string) string {
	b := make([]byte, 3)

	_, err := rand.Read(b)
	if err != nil {
		return fmt.Sprintf("%s-1", name)
	}

	return fmt.Sprintf("%s-%x", name, b)
}

func getEnvOrSkip(tb testing.TB, name string) string {
	tb.Helper()

	value := os.Getenv(name)
	if value == "" {
		tb.Skip(fmt.Sprintf("Environment variable '%s' not provided", name))
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

func getProviderOptionsAndCleanup(tb testing.TB, envName string) *Options {
	tb.Helper()

	value := getEnvOrSkip(tb, envName)

	var o Options
	if err := json.NewDecoder(strings.NewReader(value)).Decode(&o); err != nil {
		tb.Skipf("invalid credentials JSON provided in '%v'", envName)
	}

	if o.Prefix != "" {
		tb.Fatalf("options providd in '%v' must not specify a prefix", envName)
	}

	cleanupOldData(context.Background(), tb, &o, defaultCleanupAge)

	o.Prefix = uuid.NewString() + "-"

	tb.Cleanup(func() {
		cleanupOldData(context.Background(), tb, &o, 0)
	})

	return &o
}

func TestS3StorageProviders(t *testing.T) {
	t.Parallel()

	for k, env := range providerCreds {
		env := env

		t.Run(k, func(t *testing.T) {
			options := getProviderOptionsAndCleanup(t, env)

			testutil.Retry(t, func(t *testutil.RetriableT) {
				testStorage(t, options)
			})
		})
	}
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

	minioEndpoint := startDockerMinioOrSkip(t)

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

			createBucket(t, options)
			testStorage(t, options)
		})
	}
}

func TestInvalidCredsFailsFast(t *testing.T) {
	t.Parallel()

	minioEndpoint := startDockerMinioOrSkip(t)

	ctx := testlogging.Context(t)

	t0 := clock.Now()

	if _, err := New(ctx, &Options{
		Endpoint:        minioEndpoint,
		AccessKeyID:     minioAccessKeyID,
		SecretAccessKey: minioSecretAccessKey + "bad",
		BucketName:      minioBucketName,
		Region:          minioRegion,
		DoNotUseTLS:     false,
		DoNotVerifyTLS:  false,
	}); err == nil {
		t.Fatalf("unexpected success with bad credentials")
	}

	if dt := clock.Since(t0); dt > 10*time.Second {
		t.Fatalf("opening storage took too long, probably due to retries")
	}
}

func TestS3StorageMinioSTS(t *testing.T) {
	t.Parallel()

	minioEndpoint := startDockerMinioOrSkip(t)

	for _, disableTLSVerify := range []bool{true, false} {
		disableTLSVerify := disableTLSVerify

		testutil.Retry(t, func(t *testutil.RetriableT) {
			// create kopia user and session token
			kopiaUserName := generateName("kopiauser")
			kopiaUserPasswd := generateName("kopiapassword")

			createMinioUser(t, minioEndpoint, kopiaUserName, kopiaUserPasswd)
			defer deleteMinioUser(t, minioEndpoint, kopiaUserName)
			kopiaAccessKeyID, kopiaSecretKey, kopiaSessionToken := createMinioSessionToken(t, minioEndpoint, kopiaUserName, kopiaUserPasswd, minioBucketName)

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

			createBucket(t, &Options{
				Endpoint:        minioEndpoint,
				AccessKeyID:     minioAccessKeyID,
				SecretAccessKey: minioSecretAccessKey,
				BucketName:      minioBucketName,
				Region:          minioRegion,
				DoNotUseTLS:     !minioUseSSL,
				DoNotVerifyTLS:  disableTLSVerify,
			})
			testStorage(t, options)
		})
	}
}

func testStorage(t *testutil.RetriableT, options *Options) {
	ctx := testlogging.Context(t)

	data := make([]byte, 8)
	rand.Read(data)

	cleanupOldData(ctx, t, options, time.Hour)

	if options.Prefix == "" {
		options.Prefix = fmt.Sprintf("test-%v-%x-", clock.Now().Unix(), data)
	}

	attempt := func() (interface{}, error) {
		return New(testlogging.Context(t), options)
	}

	v, err := retry.WithExponentialBackoff(ctx, "New() S3 storage", attempt, func(err error) bool { return err != nil })
	if err != nil {
		t.Fatalf("err: %v, options:%v", err, options)
	}

	st := v.(blob.Storage)
	blobtesting.VerifyStorage(ctx, t.T, st)
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t.T, st)

	if err := st.Close(ctx); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestCustomTransportNoSSLVerify(t *testing.T) {
	testURL(t, expiredBadSSL)
	testURL(t, selfSignedBadSSL)
	testURL(t, untrustedRootBadSSL)
	testURL(t, wrongHostBadSSL)
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

func testURL(t *testing.T, url string) {
	t.Helper()

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
	minioClient, err := minio.New(opt.Endpoint,
		&minio.Options{
			Creds:  miniocreds.NewStaticV4(opt.AccessKeyID, opt.SecretAccessKey, ""),
			Secure: !opt.DoNotUseTLS,
			Region: opt.Region,
		})
	if err != nil {
		t.Fatalf("can't initialize minio client: %v", err)
	}

	// ignore error
	if err := minioClient.MakeBucket(context.Background(), opt.BucketName, minio.MakeBucketOptions{
		Region: opt.Region,
	}); err != nil {
		var er minio.ErrorResponse

		if errors.As(err, &er) && er.Code == "BucketAlreadyOwnedByYou" {
			return
		}

		t.Fatalf("unable to create bucket: %v", err)
	}
}

func createMinioUser(t *testutil.RetriableT, minioEndpoint, kopiaUserName, kopiaPasswd string) {
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

func deleteMinioUser(t *testutil.RetriableT, minioEndpoint, kopiaUserName string) {
	// create minio admin client
	adminCli, err := madmin.New(minioEndpoint, minioAccessKeyID, minioSecretAccessKey, minioUseSSL)
	if err != nil {
		t.Fatalf("can't initialize minio admin client: %v", err)
	}

	// delete temp kopia user
	// ignore error
	_ = adminCli.RemoveUser(testlogging.Context(t), kopiaUserName)
}

func createMinioSessionToken(t *testutil.RetriableT, minioEndpoint, kopiaUserName, kopiaUserPasswd, bucketName string) (accessID, secretKey, sessionToken string) {
	// Configure to use MinIO Server
	awsConfig := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(kopiaUserName, kopiaUserPasswd, ""),
		Endpoint:         aws.String(minioEndpoint),
		Region:           aws.String(minioRegion),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(!minioUseSSL),
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

	t.Logf("created session token with assume role: expiration: %s", result.Credentials.Expiration)

	return *result.Credentials.AccessKeyId, *result.Credentials.SecretAccessKey, *result.Credentials.SessionToken
}

func cleanupOldData(ctx context.Context, tb testing.TB, options *Options, cleanupAge time.Duration) {
	tb.Helper()

	tb.Logf("cleaning up prefix %q", options.Prefix)

	// cleanup old data from the bucket
	st, err := New(testlogging.Context(tb), options)
	if err != nil {
		tb.Fatalf("err: %v", err)
	}

	_ = st.ListBlobs(ctx, "", func(it blob.Metadata) error {
		age := clock.Since(it.Timestamp)
		if age > cleanupAge {
			if err := st.DeleteBlob(ctx, it.BlobID); err != nil {
				tb.Errorf("warning: unable to delete %q: %v", it.BlobID, err)
			}
		}
		return nil
	})
}
