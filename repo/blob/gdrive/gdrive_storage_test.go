package gdrive_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"io"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	drive "google.golang.org/api/drive/v3"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/gdrive"
)

func TestCleanupOldData(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)
	ctx := testlogging.Context(t)

	st, err := gdrive.New(ctx, mustGetOptionsOrSkip(t), false)
	require.NoError(t, err)

	defer st.Close(ctx)

	blobtesting.CleanupOldData(ctx, t, st, blobtesting.MinCleanupAge)
}

func TestGDriveStorage(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	ctx := testlogging.Context(t)

	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	opt := mustGetOptionsOrSkip(t)
	testOpt := createTestFolderOrSkip(newctx, t, opt, uuid.NewString())
	st, err := gdrive.New(newctx, testOpt, false)

	cancel()
	require.NoError(t, err)

	defer st.Close(ctx)
	defer deleteTestFolder(ctx, t, testOpt)

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})

	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

func TestGdriveStorageInvalid(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	folderID := os.Getenv("KOPIA_GDRIVE_TEST_FOLDER_ID")
	if folderID == "" {
		t.Skip("KOPIA_GDRIVE_TEST_FOLDER_ID not provided")
	}

	ctx := testlogging.Context(t)

	if _, err := gdrive.New(ctx, &gdrive.Options{
		FolderID:                      folderID + "-no-such-folder",
		ServiceAccountCredentialsFile: os.Getenv("KOPIA_GDRIVE_CREDENTIALS_FILE"),
	}, false); err == nil {
		t.Fatalf("unexpected success connecting to Drive, wanted error")
	}
}

func gunzip(d []byte) ([]byte, error) {
	z, err := gzip.NewReader(bytes.NewReader(d))
	if err != nil {
		return nil, err
	}

	defer z.Close()

	return io.ReadAll(z)
}

func mustGetOptionsOrSkip(t *testing.T) *gdrive.Options {
	t.Helper()

	folderID := os.Getenv("KOPIA_GDRIVE_TEST_FOLDER_ID")
	if folderID == "" {
		t.Skip("KOPIA_GDRIVE_TEST_FOLDER_ID not provided")
	}

	credDataGZ, err := base64.StdEncoding.DecodeString(os.Getenv("KOPIA_GDRIVE_CREDENTIALS_JSON_GZIP"))
	if err != nil {
		t.Skip("skipping test because GDrive credentials file can't be decoded")
	}

	credData, err := gunzip(credDataGZ)
	if err != nil {
		t.Skip("skipping test because GDrive credentials file can't be unzipped")
	}

	return &gdrive.Options{
		FolderID:                     folderID,
		ServiceAccountCredentialJSON: credData,
	}
}

func createTestFolderOrSkip(ctx context.Context, t *testing.T, opt *gdrive.Options, folderName string) *gdrive.Options {
	t.Helper()

	service, err := gdrive.CreateDriveService(ctx, opt)

	require.NoError(t, err)

	client := service.Files

	folder, err := client.Create(&drive.File{
		Name:     folderName,
		Parents:  []string{opt.FolderID},
		MimeType: "application/vnd.google-apps.folder",
	}).
		Context(ctx).
		Do()
	if err != nil {
		t.Skip("skipping test because test folder failed to be created")
	}

	newOpt := *opt
	newOpt.FolderID = folder.Id

	return &newOpt
}

func deleteTestFolder(ctx context.Context, t *testing.T, opt *gdrive.Options) {
	t.Helper()

	service, err := gdrive.CreateDriveService(ctx, opt)

	require.NoError(t, err)

	client := service.Files
	err = client.Delete(opt.FolderID).Context(ctx).Do()

	require.NoError(t, err)
}
