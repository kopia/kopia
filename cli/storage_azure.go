package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/azure"
)

type storageAzureFlags struct {
	azOptions azure.Options
}

func (c *storageAzureFlags) Setup(svc StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("container", "Name of the Azure blob container").Required().StringVar(&c.azOptions.Container)
	cmd.Flag("storage-account", "Azure storage account name (overrides AZURE_STORAGE_ACCOUNT environment variable)").Required().Envar(svc.EnvName("AZURE_STORAGE_ACCOUNT")).StringVar(&c.azOptions.StorageAccount)
	secretVarWithEnv(cmd.Flag("storage-key", "Azure storage account key (overrides AZURE_STORAGE_KEY environment variable)"), svc.EnvName("AZURE_STORAGE_KEY"), &c.azOptions.StorageKey)
	cmd.Flag("storage-domain", "Azure storage domain").Envar(svc.EnvName("AZURE_STORAGE_DOMAIN")).StringVar(&c.azOptions.StorageDomain)
	secretVarWithEnv(cmd.Flag("sas-token", "Azure SAS Token"), svc.EnvName("AZURE_STORAGE_SAS_TOKEN"), &c.azOptions.SASToken)
	cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&c.azOptions.Prefix)
	cmd.Flag("tenant-id", "Azure service principle tenant ID (overrides AZURE_TENANT_ID environment variable)").Envar(svc.EnvName("AZURE_TENANT_ID")).StringVar(&c.azOptions.TenantID)
	cmd.Flag("client-id", "Azure service principle client ID (overrides AZURE_CLIENT_ID environment variable)").Envar(svc.EnvName("AZURE_CLIENT_ID")).StringVar(&c.azOptions.ClientID)
	cmd.Flag("client-secret", "Azure service principle client secret (overrides AZURE_CLIENT_SECRET environment variable)").Envar(svc.EnvName("AZURE_CLIENT_SECRET")).StringVar(&c.azOptions.ClientSecret)

	commonThrottlingFlags(cmd, &c.azOptions.Limits)
}

func (c *storageAzureFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	_ = formatVersion

	//nolint:wrapcheck
	return azure.New(ctx, &c.azOptions, isCreate)
}
