package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

var (
	indexInspectCommand = indexCommands.Command("inspect", "Inpect index blob")
	indexInspectBlobIDs = indexInspectCommand.Arg("blobs", "Names of index blobs to inspect").Strings()
)

func runInspectIndexAction(ctx context.Context, rep repo.DirectRepository) error {
	for _, indexBlobID := range *indexInspectBlobIDs {
		if err := inspectSingleIndexBlob(ctx, rep, blob.ID(indexBlobID)); err != nil {
			return err
		}
	}

	return nil
}

func dumpIndexBlobEntries(bm blob.Metadata, entries []content.Info) {
	for _, ci := range entries {
		state := "created"
		if ci.GetDeleted() {
			state = "deleted"
		}

		printStdout("%v %v %v %v %v %v %v %v\n",
			formatTimestampPrecise(bm.Timestamp), bm.BlobID,
			ci.GetContentID(), state, formatTimestampPrecise(ci.Timestamp()), ci.GetPackBlobID(), ci.GetPackOffset(), ci.GetPackedLength())
	}
}

func inspectSingleIndexBlob(ctx context.Context, rep repo.DirectRepository, blobID blob.ID) error {
	bm, err := rep.BlobReader().GetMetadata(ctx, blobID)
	if err != nil {
		return errors.Wrapf(err, "unable to get metadata for %v", blobID)
	}

	entries, err := rep.IndexBlobReader().ParseIndexBlob(ctx, blobID)
	if err != nil {
		return errors.Wrapf(err, "unable to recover index from %v", blobID)
	}

	dumpIndexBlobEntries(bm, entries)

	return nil
}

func init() {
	indexInspectCommand.Action(directRepositoryReadAction(runInspectIndexAction))
}
