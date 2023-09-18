package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/repo"
)

type commandRepositoryValidateProvider struct {
	opt providervalidation.Options
	out textOutput
}

func (c *commandRepositoryValidateProvider) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("validate-provider", "Validates that a repository provider is compatible with Kopia")

	c.opt = providervalidation.DefaultOptions

	cmd.Flag("num-storage-connections", "Number of storage connections").IntVar(&c.opt.NumEquivalentStorageConnections)
	cmd.Flag("concurrency-test-duration", "Duration of concurrency test").DurationVar(&c.opt.ConcurrencyTestDuration)
	cmd.Flag("put-blob-workers", "Number of PutBlob workers").IntVar(&c.opt.NumPutBlobWorkers)
	cmd.Flag("get-blob-workers", "Number of GetBlob workers").IntVar(&c.opt.NumGetBlobWorkers)
	cmd.Flag("get-metadata-workers", "Number of GetMetadata workers").IntVar(&c.opt.NumGetMetadataWorkers)
	c.out.setup(svc)

	cmd.Action(c.out.svc.directRepositoryWriteAction(c.run))
}

func (c *commandRepositoryValidateProvider) run(ctx context.Context, dr repo.DirectRepositoryWriter) error {
	return errors.Wrap(
		providervalidation.ValidateProvider(ctx, dr.BlobStorage(), c.opt),
		"provider validation error")
}
