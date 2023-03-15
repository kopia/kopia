package cli

import (
	"context"
	"encoding/json"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/gcs"
)

type storageGCSFlags struct {
	options gcs.Options

	embedCredentials bool
}

func (c *storageGCSFlags) Setup(_ StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("bucket", "Name of the Google Cloud Storage bucket").Required().StringVar(&c.options.BucketName)
	cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&c.options.Prefix)
	cmd.Flag("read-only", "Use read-only GCS scope to prevent write access").BoolVar(&c.options.ReadOnly)
	cmd.Flag("credentials-file", "Use the provided JSON file with credentials").ExistingFileVar(&c.options.ServiceAccountCredentialsFile)
	cmd.Flag("embed-credentials", "Embed GCS credentials JSON in Kopia configuration").BoolVar(&c.embedCredentials)

	commonThrottlingFlags(cmd, &c.options.Limits)
}

func (c *storageGCSFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	if c.embedCredentials {
		data, err := os.ReadFile(c.options.ServiceAccountCredentialsFile)
		if err != nil {
			return nil, errors.Wrap(err, "unable to open service account credentials file")
		}

		c.options.ServiceAccountCredentialJSON = json.RawMessage(data)
		c.options.ServiceAccountCredentialsFile = ""
	}

	//nolint:wrapcheck
	return gcs.New(ctx, &c.options, false)
}
