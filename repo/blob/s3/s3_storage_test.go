package s3

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/tlsutil"
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
func startDockerMinioOrSkip(t *testing.T, minioConfigDir string) string {
	t.Helper()

	testutil.TestSkipOnCIUnlessLinuxAMD64(t)

	containerID := testutil.RunContainerAndKillOnCloseOrSkip(t,
		"run", "--rm", "-p", "0:9000",
		"-e", "MINIO_ROOT_USER="+minioRootAccessKeyID,
		"-e", "MINIO_ROOT_PASSWORD="+minioRootSecretAccessKey,
		"-e", "MINIO_REGION_NAME="+minioRegion,
		"-v", minioConfigDir+":/root/.minio",
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

			testStorage(t, opt, false, blob.PutOptions{})
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

	getOrCreateBucket(t, options)
	testStorage(t, options, false, blob.PutOptions{})
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
	getOrCreateBucket(t, &Options{
		Endpoint:        getEnv(testEndpointEnv, awsEndpoint),
		AccessKeyID:     getEnv(testAccessKeyIDEnv, ""),
		SecretAccessKey: getEnv(testSecretAccessKeyEnv, ""),
		BucketName:      options.BucketName,
		Region:          options.Region,
	})
	testStorage(t, options, false, blob.PutOptions{})
}

func TestS3StorageRetentionUnlockedBucket(t *testing.T) {
	t.Parallel()

	// skip the test if AWS creds are not provided
	options := Options{
		Endpoint:        getEnv(testEndpointEnv, awsEndpoint),
		AccessKeyID:     getEnvOrSkip(t, testAccessKeyIDEnv),
		SecretAccessKey: getEnvOrSkip(t, testSecretAccessKeyEnv),
		BucketName:      getEnvOrSkip(t, testBucketEnv),
		Region:          getEnvOrSkip(t, testRegionEnv),
	}

	getOrCreateBucket(t, &options)

	t.Run("valid period", func(t *testing.T) {
		// expected to fail on non-locked buckets
		testPutBlobWithInvalidRetention(t, options, blob.PutOptions{
			RetentionMode:   minio.Governance.String(),
			RetentionPeriod: time.Hour * 24,
		})
	})

	t.Run("invalid period", func(t *testing.T) {
		options.Prefix = ""
		testPutBlobWithInvalidRetention(t, options, blob.PutOptions{
			RetentionMode:   minio.Governance.String(),
			RetentionPeriod: time.Nanosecond,
		})
	})
}

func TestS3StorageRetentionLockedBucket(t *testing.T) {
	t.Parallel()

	// skip the test if AWS creds are not provided
	options := Options{
		Endpoint:        getEnv(testEndpointEnv, awsEndpoint),
		AccessKeyID:     getEnvOrSkip(t, testAccessKeyIDEnv),
		SecretAccessKey: getEnvOrSkip(t, testSecretAccessKeyEnv),
		BucketName:      getEnvOrSkip(t, testLockedBucketEnv),
		Region:          getEnvOrSkip(t, testRegionEnv),
	}

	getOrCreateBucket(t, &options)

	t.Run("testStorage", func(t *testing.T) {
		testStorage(t, &options, false, blob.PutOptions{
			RetentionMode:   minio.Governance.String(),
			RetentionPeriod: time.Hour * 24,
		})
	})

	t.Run("invalid period", func(t *testing.T) {
		options.Prefix = ""
		testPutBlobWithInvalidRetention(t, options, blob.PutOptions{
			RetentionMode:   minio.Governance.String(),
			RetentionPeriod: time.Nanosecond,
		})
	})
}

func TestS3StorageMinio(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	minioEndpoint := startDockerMinioOrSkip(t, testutil.TempDirectory(t))

	options := &Options{
		Endpoint:        minioEndpoint,
		AccessKeyID:     minioRootAccessKeyID,
		SecretAccessKey: minioRootSecretAccessKey,
		BucketName:      minioBucketName,
		Region:          minioRegion,
		DoNotUseTLS:     true,
	}

	createBucket(t, options)
	testStorage(t, options, true, blob.PutOptions{})
}

func TestS3StorageMinioSelfSignedCert(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	ctx := testlogging.Context(t)
	minioConfigDir := testutil.TempDirectory(t)
	certsDir := filepath.Join(minioConfigDir, "certs")
	require.NoError(t, os.MkdirAll(certsDir, 0o755))

	cert, key, err := tlsutil.GenerateServerCertificate(
		ctx,
		2048,
		24*time.Hour,
		[]string{"myhost"})

	require.NoError(t, err)

	require.NoError(t, tlsutil.WriteCertificateToFile(filepath.Join(certsDir, "public.crt"), cert))
	require.NoError(t, tlsutil.WritePrivateKeyToFile(filepath.Join(certsDir, "private.key"), key))

	minioEndpoint := startDockerMinioOrSkip(t, minioConfigDir)

	options := &Options{
		Endpoint:        minioEndpoint,
		AccessKeyID:     minioRootAccessKeyID,
		SecretAccessKey: minioRootSecretAccessKey,
		BucketName:      minioBucketName,
		Region:          minioRegion,
		DoNotVerifyTLS:  true,
	}

	createBucket(t, options)
	testStorage(t, options, true, blob.PutOptions{})
}

func TestInvalidCredsFailsFast(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	minioEndpoint := startDockerMinioOrSkip(t, testutil.TempDirectory(t))

	ctx := testlogging.Context(t)

	timer := timetrack.StartTimer()

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

	// nolint:forbidigo
	if dt := timer.Elapsed(); dt > 10*time.Second {
		t.Fatalf("opening storage took too long, probably due to retries")
	}
}

func TestS3StorageMinioSTS(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	minioEndpoint := startDockerMinioOrSkip(t, testutil.TempDirectory(t))

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
	}, true, blob.PutOptions{})
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
	getOrMakeBucket(t, cli, options, true)

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
		blobtesting.CleanupOldData(ctx, t, s, 0)
	})

	err = s.PutBlob(ctx, blob.ID("test-put-blob-0"), gather.FromSlice([]byte("xxyasdf243z")), blob.PutOptions{})

	require.NoError(t, err, "could not put test blob")
}

// nolint:thelper
func testStorage(t *testing.T, options *Options, runValidationTest bool, opts blob.PutOptions) {
	ctx := testlogging.Context(t)

	require.Equal(t, "", options.Prefix)

	st0, err := New(ctx, options)
	require.NoError(t, err)

	defer st0.Close(ctx)

	blobtesting.CleanupOldData(ctx, t, st0, blobtesting.MinCleanupAge)

	options.Prefix = uuid.NewString()

	st, err := New(ctx, options)
	require.NoError(t, err)

	defer st.Close(ctx)
	defer blobtesting.CleanupOldData(ctx, t, st, 0)

	blobtesting.VerifyStorage(ctx, t, st, opts)
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)

	if runValidationTest {
		require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
	}
}

// nolint:thelper,gocritic
func testPutBlobWithInvalidRetention(t *testing.T, options Options, opts blob.PutOptions) {
	ctx := testlogging.Context(t)

	require.Equal(t, "", options.Prefix)
	options.Prefix = uuid.NewString()

	// non-retrying storage
	st, err := newStorage(ctx, &options)
	require.NoError(t, err)

	defer st.Close(ctx)
	defer blobtesting.CleanupOldData(ctx, t, st, 0)

	// Now attempt to add a block and expect to fail
	require.Error(t,
		st.PutBlob(ctx, blob.ID("abcdbbf4f0507d054ed5a80a5b65086f602b"), gather.FromSlice([]byte{}), opts))

	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
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

	var transport http.RoundTripper

	if opt.DoNotVerifyTLS {
		transport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	}

	minioClient, err := minio.New(opt.Endpoint,
		&minio.Options{
			Creds:     miniocreds.NewStaticV4(opt.AccessKeyID, opt.SecretAccessKey, ""),
			Secure:    !opt.DoNotUseTLS,
			Region:    opt.Region,
			Transport: transport,
		})
	if err != nil {
		tb.Fatalf("can't initialize minio client: %v", err)
	}

	return minioClient
}

func getOrCreateBucket(tb testing.TB, opt *Options) {
	tb.Helper()

	minioClient := createClient(tb, opt)

	getOrMakeBucket(tb, minioClient, opt, false)
}

func createBucket(tb testing.TB, opt *Options) {
	tb.Helper()

	minioClient := createClient(tb, opt)

	makeBucket(tb, minioClient, opt, false)
}

func getOrMakeBucket(tb testing.TB, cli *minio.Client, opt *Options, objectLocking bool) {
	tb.Helper()

	ctx := testlogging.Context(tb)

	// check whether the bucket exists before attempting to create it to avoid
	// and reduce the overall number of potentially expensive bucket creation
	// calls.
	if loc, err := cli.GetBucketLocation(ctx, opt.BucketName); err == nil {
		tb.Log("found bucket", opt.BucketName, "in location", loc)

		return
	}

	makeBucket(tb, cli, opt, objectLocking)
}

func makeBucket(tb testing.TB, cli *minio.Client, opt *Options, objectLocking bool) {
	tb.Helper()

	ctx := testlogging.Context(tb)

	if err := cli.MakeBucket(ctx, opt.BucketName, minio.MakeBucketOptions{
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
