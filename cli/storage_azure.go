package cli

import (
	"context"

	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/azure"
)

type storageAzureFlags struct {
	azOptions azure.Options
}

func (c *storageAzureFlags) setup(_ storageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("container", "Name of the Azure blob container").Required().StringVar(&c.azOptions.Container)
	cmd.Flag("storage-account", "Azure storage account name(overrides AZURE_STORAGE_ACCOUNT environment variable)").Required().Envar("AZURE_STORAGE_ACCOUNT").StringVar(&c.azOptions.StorageAccount)
	cmd.Flag("storage-key", "Azure storage account key(overrides AZURE_STORAGE_KEY environment variable)").Required().Envar("AZURE_STORAGE_KEY").StringVar(&c.azOptions.StorageKey)
	cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&c.azOptions.Prefix)
	cmd.Flag("max-download-speed", "Limit the download speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&c.azOptions.MaxDownloadSpeedBytesPerSecond)
	cmd.Flag("max-upload-speed", "Limit the upload speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&c.azOptions.MaxUploadSpeedBytesPerSecond)
}

func (c *storageAzureFlags) connect(ctx context.Context, isNew bool) (blob.Storage, error) {
	// nolint:wrapcheck
	return azure.New(ctx, &c.azOptions)
}
