package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gdriveauth"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/gdrive"
	"github.com/pkg/browser"
)

type storageGDriveFlags struct {
	options gdrive.Options

	embedCredentials bool
}

func (c *storageGDriveFlags) Setup(_ StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("app-id", "Google Cloud Project ID").StringVar(&c.options.AppId)
	cmd.Flag("api-key", "Google Cloud API Key").StringVar(&c.options.ApiKey)
	cmd.Flag("client-id", "Google Cloud OAuth Client ID").StringVar(&c.options.ClientId)
	cmd.Flag("client-secret", "Google Cloud OAuth Client Secret").StringVar(&c.options.ClientSecret)
	// Deprecated flags for accessing GDrive with service account.
	cmd.Flag("folder-id", "FolderID to use for objects in the bucket (deprecated)").StringVar(&c.options.FolderID)
	cmd.Flag("credentials-file", "Use the provided JSON file with credentials (deprecated)").ExistingFileVar(&c.options.ServiceAccountCredentialsFile)
	cmd.Flag("embed-credentials", "Embed GCS credentials JSON in Kopia configuration (deprecated)").BoolVar(&c.embedCredentials)

	commonThrottlingFlags(cmd, &c.options.Limits)
}

func (c *storageGDriveFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	_ = formatVersion
	opts := c.options

	if opts.AppId != "" || opts.ApiKey != "" || opts.ClientId != "" || opts.ClientSecret != "" {
		if opts.AppId == "" || opts.ApiKey == "" || opts.ClientId == "" || opts.ClientSecret == "" {
			return nil, errors.New("api-id, api-key, client-id and client-secret must be specified")
		}

		result := make(chan gdriveauth.Result)

		go func() {
			gdriveauth.New(&opts.OAuthConfig, result)
		}()

		browser.OpenURL("http://localhost:8080")
		fmt.Println("Continue in the browser to select a Google Drive folder to back up to.")
		fmt.Println("If a new browser window is not opened, go to http://localhost:8080.")

		res, ok := <-result
		if !ok {
			return nil, errors.New("Auth not successful")
		}
		c.options.OAuthToken = res.Token
		c.options.FolderID = res.FolderId

	} else if c.embedCredentials {
		data, err := os.ReadFile(opts.ServiceAccountCredentialsFile)
		if err != nil {
			return nil, errors.Wrap(err, "unable to open service account credentials file")
		}

		c.options.ServiceAccountCredentialJSON = json.RawMessage(data)
		c.options.ServiceAccountCredentialsFile = ""
	}

	//nolint:wrapcheck
	return gdrive.New(ctx, &c.options, isCreate)
}
