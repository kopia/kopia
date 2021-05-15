package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

type commandBlobList struct {
	blobListPrefix  string
	blobListMinSize int64
	blobListMaxSize int64

	jo  jsonOutput
	out textOutput
}

func (c *commandBlobList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List BLOBs").Alias("ls")
	cmd.Flag("prefix", "Blob ID prefix").StringVar(&c.blobListPrefix)
	cmd.Flag("min-size", "Minimum size").Int64Var(&c.blobListMinSize)
	cmd.Flag("max-size", "Maximum size").Int64Var(&c.blobListMaxSize)
	c.jo.setup(svc, cmd)
	c.out.setup(svc)
	cmd.Action(svc.directRepositoryReadAction(c.run))
}

func (c *commandBlobList) run(ctx context.Context, rep repo.DirectRepository) error {
	var jl jsonList

	jl.begin(&c.jo)
	defer jl.end()

	// nolint:wrapcheck
	return rep.BlobReader().ListBlobs(ctx, blob.ID(c.blobListPrefix), func(b blob.Metadata) error {
		if c.blobListMaxSize != 0 && b.Length > c.blobListMaxSize {
			return nil
		}

		if c.blobListMinSize != 0 && b.Length < c.blobListMinSize {
			return nil
		}

		if c.jo.jsonOutput {
			jl.emit(b)
		} else {
			c.out.printStdout("%-70v %10v %v\n", b.BlobID, b.Length, formatTimestamp(b.Timestamp))
		}

		return nil
	})
}
