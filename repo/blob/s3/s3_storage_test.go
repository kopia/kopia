package s3

import (
	"context"
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
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
)

const (
	// https://github.com/minio/minio-go

	// fake creadentials used by minio server we're launching.
	minioRootAccessKeyID     = "fake-key"
	minioRootSecretAccessKey = "fake-secret"
	minioRegion              = "fake-region-1"
	minioBucketName          = "my-bucket" // we use ephemeral minio for each test so this does not need to be unique

	// default aws S3 endpoint.
	awsEndpoint = "s3.amazonaws.com"

	// env vars need to be set to execute TestS3StorageAWS.
	testEndpointEnv        = "KOPIA_S3_TEST_ENDPOINT"
	testAccessKeyIDEnv     = "KOPIA_S3_TEST_ACCESS_KEY_ID"
	testSecretAccessKeyEnv = "KOPIA_S3_TEST_SECRET_ACCESS_KEY"
	testBucketEnv          = "KOPIA_S3_TEST_BUCKET"
	testLockedBucketEnv    = "KOPIA_S3_TEST_LOCKED_BUCKET"
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
	"S3-Versioned":     "KOPIA_S3_VERSIONED_CREDS",
	"Wasabi":           "KOPIA_S3_WASABI_CREDS",
	"Wasabi-Versioned": "KOPIA_S3_WASABI_VERSIONED_CREDS",
}

// startDockerMinioOrSkip starts ephemeral minio instance on a random port and returns the endpoint ("localhost:xxx").
func startDockerMinioOrSkip(t *testing.T) string {
	t.Helper()

	testutil.TestSkipOnCIUnlessLinuxAMD64(t)

	containerID := testutil.RunContainerAndKillOnCloseOrSkip(t,
		"run", "--rm", "-p", "0:9000",
		"-e", "MINIO_ROOT_USER="+minioRootAccessKeyID,
		"-e", "MINIO_ROOT_PASSWORD="+minioRootSecretAccessKey,
		"-e", "MINIO_REGION_NAME="+minioRegion,
		"-d", "minio/minio", "server", "/data")
	endpoint := testutil.GetContainerMappedPortAddress(t, containerID, "9000")

	t.Logf("endpoint: %v", endpoint)

	return endpoint
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

func getProviderOptions(tb testing.TB, envName string) *Options {
	tb.Helper()

	value := getEnvOrSkip(tb, envName)

	var o Options
	if err := json.NewDecoder(strings.NewReader(value)).Decode(&o); err != nil {
		tb.Skipf("invalid credentials JSON provided in '%v'", envName)
	}

	if o.Prefix != "" {
		tb.Fatalf("options providd in '%v' must not specify a prefix", envName)
	}

	return &o
}

func TestS3StorageProviders(t *testing.T) {
	t.Parallel()

	for k, env := range providerCreds {
		env := env

		t.Run(k, func(t *testing.T) {
			opt := getProviderOptions(t, env)

			testStorage(t, opt)
		})
	}
}

func TestS3StorageAWS(t *testing.T) {
	t.Parallel()

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
}

func TestS3StorageAWSSTS(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

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
}

func TestS3StorageMinio(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	minioEndpoint := startDockerMinioOrSkip(t)

	options := &Options{
		Endpoint:        minioEndpoint,
		AccessKeyID:     minioRootAccessKeyID,
		SecretAccessKey: minioRootSecretAccessKey,
		BucketName:      minioBucketName,
		Region:          minioRegion,
		DoNotUseTLS:     true,
	}

	createBucket(t, options)
	testStorage(t, options)
}

func TestInvalidCredsFailsFast(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	minioEndpoint := startDockerMinioOrSkip(t)

	ctx := testlogging.Context(t)

	t0 := clock.Now()

	_, err := New(ctx, &Options{
		Endpoint:        minioEndpoint,
		AccessKeyID:     minioRootAccessKeyID,
		SecretAccessKey: minioRootSecretAccessKey + "bad",
		BucketName:      minioBucketName,
		Region:          minioRegion,
		DoNotUseTLS:     false,
		DoNotVerifyTLS:  false,
	})
	require.Error(t, err)

	if dt := clock.Since(t0); dt > 10*time.Second {
		t.Fatalf("opening storage took too long, probably due to retries")
	}
}

func TestS3StorageMinioSTS(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	minioEndpoint := startDockerMinioOrSkip(t)

	time.Sleep(2 * time.Second)

	kopiaAccessKeyID, kopiaSecretKey, kopiaSessionToken := createMinioSessionToken(t, minioEndpoint, minioRootAccessKeyID, minioRootSecretAccessKey, minioBucketName)

	createBucket(t, &Options{
		Endpoint:        minioEndpoint,
		AccessKeyID:     minioRootAccessKeyID,
		SecretAccessKey: minioRootSecretAccessKey,
		BucketName:      minioBucketName,
		Region:          minioRegion,
		DoNotUseTLS:     true,
	})

	require.NotEqual(t, kopiaAccessKeyID, minioRootAccessKeyID)
	require.NotEqual(t, kopiaSecretKey, minioRootSecretAccessKey)

	testStorage(t, &Options{
		Endpoint:        minioEndpoint,
		AccessKeyID:     kopiaAccessKeyID,
		SecretAccessKey: kopiaSecretKey,
		SessionToken:    kopiaSessionToken,
		BucketName:      minioBucketName,
		Region:          minioRegion,
		DoNotUseTLS:     true,
	})
}

func TestNeedMD5AWS(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	// skip the test if AWS creds are not provided
	options := &Options{
		Endpoint:        getEnv(testEndpointEnv, awsEndpoint),
		AccessKeyID:     getEnvOrSkip(t, testAccessKeyIDEnv),
		SecretAccessKey: getEnvOrSkip(t, testSecretAccessKeyEnv),
		BucketName:      getEnvOrSkip(t, testLockedBucketEnv),
		Region:          getEnvOrSkip(t, testRegionEnv),
	}

	ctx := testlogging.Context(t)
	cli := createClient(t, options)
	makeBucket(t, cli, options, true)

	// ensure it is a bucket with object locking enabled
	want := "Enabled"
	if got, _, _, _, _ := cli.GetObjectLockConfig(ctx, options.BucketName); got != want {
		t.Fatalf("object locking is not enabled: got '%s', want '%s'", got, want)
	}

	// ensure a locking configuration is in place
	lockingMode := minio.Governance
	unit := uint(1)
	days := minio.Days
	err := cli.SetBucketObjectLockConfig(ctx, options.BucketName, &lockingMode, &unit, &days)
	require.NoError(t, err, "could not set object lock config")

	options.Prefix = uuid.NewString() + "/"

	s, err := New(ctx, options)
	require.NoError(t, err, "could not create storage")

	t.Cleanup(func() {
		blobtesting.CleanupOldData(context.Background(), t, s, 0)
	})

	err = s.PutBlob(ctx, blob.ID("test-put-blob-0"), gather.FromSlice([]byte("xxyasdf243z")))

	require.NoError(t, err, "could not put test blob")
}

// nolint:thelper
func testStorage(t *testing.T, options *Options) {
	ctx := testlogging.Context(t)

	require.Equal(t, "", options.Prefix)

	st0, err := New(testlogging.Context(t), options)
	require.NoError(t, err)

	defer st0.Close(ctx)

	blobtesting.CleanupOldData(ctx, t, st0, blobtesting.MinCleanupAge)

	options.Prefix = uuid.NewString()

	st, err := New(testlogging.Context(t), options)
	require.NoError(t, err)

	defer st.Close(ctx)
	defer blobtesting.CleanupOldData(ctx, t, st, 0)

	blobtesting.VerifyStorage(ctx, t, st)
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

func TestCustomTransportNoSSLVerify(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

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

func createClient(tb testing.TB, opt *Options) *minio.Client {
	tb.Helper()

	minioClient, err := minio.New(opt.Endpoint,
		&minio.Options{
			Creds:  miniocreds.NewStaticV4(opt.AccessKeyID, opt.SecretAccessKey, ""),
			Secure: !opt.DoNotUseTLS,
			Region: opt.Region,
		})
	if err != nil {
		tb.Fatalf("can't initialize minio client: %v", err)
	}

	return minioClient
}

func createBucket(tb testing.TB, opt *Options) {
	tb.Helper()

	minioClient := createClient(tb, opt)

	makeBucket(tb, minioClient, opt, false)
}

func makeBucket(tb testing.TB, cli *minio.Client, opt *Options, objectLocking bool) {
	tb.Helper()

	if err := cli.MakeBucket(context.Background(), opt.BucketName, minio.MakeBucketOptions{
		Region:        opt.Region,
		ObjectLocking: objectLocking,
	}); err != nil {
		var er minio.ErrorResponse

		if errors.As(err, &er) && er.Code == "BucketAlreadyOwnedByYou" {
			// ignore error
			return
		}

		tb.Fatalf("unable to create bucket: %v", err)
	}
}

func createMinioSessionToken(t *testing.T, minioEndpoint, kopiaUserName, kopiaUserPasswd, bucketName string) (accessID, secretKey, sessionToken string) {
	t.Helper()

	// Configure to use MinIO Server
	awsConfig := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(kopiaUserName, kopiaUserPasswd, ""),
		Endpoint:         aws.String(minioEndpoint),
		Region:           aws.String(minioRegion),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(true),
	}

	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		t.Fatalf("failed to create aws session: %v", err)
	}

	svc := sts.New(awsSession)

	input := &sts.AssumeRoleInput{
		// give access to only S3 bucket with name bucketName
		Policy: aws.String(fmt.Sprintf(`{
			"Version":"2012-10-17",
			"Statement":[
				{
					"Sid": "ReadBucket",
					"Effect": "Allow",
					"Action": "s3:ListBucket",
					"Resource": "arn:aws:s3:::%v"
				  },
				  {
					"Sid": "AllowFullAccessInBucket",
					"Effect": "Allow",
					"Action": "s3:*",
					"Resource": "arn:aws:s3:::%v/*"
				  }
			]}`, bucketName, bucketName)),
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
