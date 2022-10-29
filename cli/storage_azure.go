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

func (c *storageAzureFlags) Setup(svc StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("container", "Name of the Azure blob container").Required().StringVar(&c.azOptions.Container)
	cmd.Flag("storage-account", "Azure storage account name (overrides AZURE_STORAGE_ACCOUNT environment variable)").Required().Envar(svc.EnvName("AZURE_STORAGE_ACCOUNT")).StringVar(&c.azOptions.StorageAccount)
	cmd.Flag("storage-key", "Azure storage account key (overrides AZURE_STORAGE_KEY environment variable)").Envar(svc.EnvName("AZURE_STORAGE_KEY")).StringVar(&c.azOptions.StorageKey)
	cmd.Flag("storage-domain", "Azure storage domain").Envar(svc.EnvName("AZURE_STORAGE_DOMAIN")).StringVar(&c.azOptions.StorageDomain)
	cmd.Flag("sas-token", "Azure SAS Token").Envar(svc.EnvName("AZURE_STORAGE_SAS_TOKEN")).StringVar(&c.azOptions.SASToken)
	cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&c.azOptions.Prefix)

	commonThrottlingFlags(cmd, &c.azOptions.Limits)
}

func (c *storageAzureFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	//nolint:wrapcheck
	return azure.New(ctx, &c.azOptions, false)
}
