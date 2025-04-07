package cli

import (
	"context"
	"encoding/json"
	"os"
	"time"

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

	var pointInTimeStr string

	pitPreAction := func(_ *kingpin.ParseContext) error {
		if pointInTimeStr != "" {
			t, err := time.Parse(time.RFC3339, pointInTimeStr)
			if err != nil {
				return errors.Wrap(err, "invalid point-in-time argument")
			}

			c.options.PointInTime = &t
		}

		return nil
	}

	cmd.Flag("point-in-time", "Use a point-in-time view of the storage repository when supported").PlaceHolder(time.RFC3339).PreAction(pitPreAction).StringVar(&pointInTimeStr)
}

func (c *storageGCSFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	_ = formatVersion

	if isCreate && c.options.PointInTime != nil && !c.options.PointInTime.IsZero() {
		return nil, errors.New("Cannot specify a 'point-in-time' option when creating a repository")
	}

	if c.embedCredentials {
		data, err := os.ReadFile(c.options.ServiceAccountCredentialsFile)
		if err != nil {
			return nil, errors.Wrap(err, "unable to open service account credentials file")
		}

		c.options.ServiceAccountCredentialJSON = json.RawMessage(data)
		c.options.ServiceAccountCredentialsFile = ""
	}

	//nolint:wrapcheck
	return gcs.New(ctx, &c.options, isCreate)
}
