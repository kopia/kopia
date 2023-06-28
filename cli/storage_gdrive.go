package cli

import (
	"context"
	"encoding/json"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/gdrive"
)

type storageGDriveFlags struct {
	options gdrive.Options

	embedCredentials bool
}

func (c *storageGDriveFlags) Setup(_ StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("folder-id", "FolderID to use for objects in the bucket").Required().StringVar(&c.options.FolderID)
	cmd.Flag("read-only", "Use read-only scope to prevent write access").BoolVar(&c.options.ReadOnly)
	cmd.Flag("credentials-file", "Use the provided JSON file with credentials").ExistingFileVar(&c.options.ServiceAccountCredentialsFile)
	cmd.Flag("embed-credentials", "Embed GCS credentials JSON in Kopia configuration").BoolVar(&c.embedCredentials)

	commonThrottlingFlags(cmd, &c.options.Limits)
}

func (c *storageGDriveFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	_ = formatVersion

	if c.embedCredentials {
		data, err := os.ReadFile(c.options.ServiceAccountCredentialsFile)
		if err != nil {
			return nil, errors.Wrap(err, "unable to open service account credentials file")
		}

		c.options.ServiceAccountCredentialJSON = json.RawMessage(data)
		c.options.ServiceAccountCredentialsFile = ""
	}

	//nolint:wrapcheck
	return gdrive.New(ctx, &c.options, isCreate)
}
