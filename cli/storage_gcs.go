package cli

import (
	"context"
	"encoding/json"
	"os"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/gcs"
)

type storageGCSFlags struct {
	options gcs.Options

	embedCredentials bool
}

func (c *storageGCSFlags) setup(_ storageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("bucket", "Name of the Google Cloud Storage bucket").Required().StringVar(&c.options.BucketName)
	cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&c.options.Prefix)
	cmd.Flag("read-only", "Use read-only GCS scope to prevent write access").BoolVar(&c.options.ReadOnly)
	cmd.Flag("credentials-file", "Use the provided JSON file with credentials").ExistingFileVar(&c.options.ServiceAccountCredentialsFile)
	cmd.Flag("max-download-speed", "Limit the download speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&c.options.MaxDownloadSpeedBytesPerSecond)
	cmd.Flag("max-upload-speed", "Limit the upload speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&c.options.MaxUploadSpeedBytesPerSecond)
	cmd.Flag("embed-credentials", "Embed GCS credentials JSON in Kopia configuration").BoolVar(&c.embedCredentials)
}

func (c *storageGCSFlags) connect(ctx context.Context, isNew bool) (blob.Storage, error) {
	if c.embedCredentials {
		data, err := os.ReadFile(c.options.ServiceAccountCredentialsFile)
		if err != nil {
			return nil, errors.Wrap(err, "unable to open service account credentials file")
		}

		c.options.ServiceAccountCredentialJSON = json.RawMessage(data)
		c.options.ServiceAccountCredentialsFile = ""
	}

	// nolint:wrapcheck
	return gcs.New(ctx, &c.options)
}
