package cli

import (
	"context"
	"strings"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/repodiag"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/indexblob"
)

type commandBlobList struct {
	blobListPrefix        string
	blobListPrefixExclude []string
	blobListMinSize       int64
	blobListMaxSize       int64
	dataOnly              bool

	jo  jsonOutput
	out textOutput
}

func (c *commandBlobList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List BLOBs").Alias("ls")
	cmd.Flag("prefix", "Blob ID prefix").StringVar(&c.blobListPrefix)
	cmd.Flag("exclude-prefix", "Blob ID prefixes to exclude").StringsVar(&c.blobListPrefixExclude)
	cmd.Flag("min-size", "Minimum size").Int64Var(&c.blobListMinSize)
	cmd.Flag("max-size", "Maximum size").Int64Var(&c.blobListMaxSize)
	cmd.Flag("data-only", "Only list data blobs").BoolVar(&c.dataOnly)
	c.jo.setup(svc, cmd)
	c.out.setup(svc)
	cmd.Action(svc.directRepositoryReadAction(c.run))
}

func (c *commandBlobList) run(ctx context.Context, rep repo.DirectRepository) error {
	var jl jsonList

	jl.begin(&c.jo)
	defer jl.end()

	//nolint:wrapcheck
	return rep.BlobReader().ListBlobs(ctx, blob.ID(c.blobListPrefix), func(b blob.Metadata) error {
		if !c.shouldInclude(b) {
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

func (c *commandBlobList) shouldInclude(b blob.Metadata) bool {
	if c.dataOnly {
		if strings.HasPrefix(string(b.BlobID), indexblob.V0IndexBlobPrefix) {
			return false
		}

		if strings.HasPrefix(string(b.BlobID), epoch.EpochManagerIndexUberPrefix) {
			return false
		}

		if strings.HasPrefix(string(b.BlobID), repodiag.LogBlobPrefix) {
			return false
		}

		if strings.HasPrefix(string(b.BlobID), "kopia.") {
			return false
		}
	}

	if c.blobListMaxSize != 0 && b.Length > c.blobListMaxSize {
		return false
	}

	if c.blobListMinSize != 0 && b.Length < c.blobListMinSize {
		return false
	}

	for _, ex := range c.blobListPrefixExclude {
		if strings.HasPrefix(string(b.BlobID), ex) {
			return false
		}
	}

	return true
}
