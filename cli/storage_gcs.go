package cli

import (
	"context"
	"encoding/json"
	"io/ioutil"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/gcs"
)

func init() {
	var options gcs.Options

	var embedCredentials bool

	RegisterStorageConnectFlags(
		"google",
		"a Google Cloud Storage bucket",
		func(cmd *kingpin.CmdClause) {
			cmd.Flag("bucket", "Name of the Google Cloud Storage bucket").Required().StringVar(&options.BucketName)
			cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&options.Prefix)
			cmd.Flag("read-only", "Use read-only GCS scope to prevent write access").BoolVar(&options.ReadOnly)
			cmd.Flag("credentials-file", "Use the provided JSON file with credentials").ExistingFileVar(&options.ServiceAccountCredentialsFile)
			cmd.Flag("max-download-speed", "Limit the download speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&options.MaxDownloadSpeedBytesPerSecond)
			cmd.Flag("max-upload-speed", "Limit the upload speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&options.MaxUploadSpeedBytesPerSecond)
			cmd.Flag("embed-credentials", "Embed GCS credentials JSON in Kopia configuration").BoolVar(&embedCredentials)
		},
		func(ctx context.Context, isNew bool) (blob.Storage, error) {
			if embedCredentials {
				data, err := ioutil.ReadFile(options.ServiceAccountCredentialsFile)
				if err != nil {
					return nil, errors.Wrap(err, "unable to open service account credentials file")
				}

				options.ServiceAccountCredentialJSON = json.RawMessage(data)
				options.ServiceAccountCredentialsFile = ""
			}

			return gcs.New(ctx, &options)
		},
	)
}
