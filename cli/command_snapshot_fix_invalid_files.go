package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

type commandSnapshotFixInvalidFiles struct {
	common commonRewriteSnapshots

	verifyFilesPercent float64
	verifier           *snapshotfs.Verifier

	invalidFileHandling string

	failedFileCallback snapshotfs.RewriteFailedEntryCallback
}

const (
	fixInvalidFilesHelp = `Remove references to any invalid (unreadable) files from snapshots.
It is only recommended to use this command if
$ kopia snapshot verify
is returning errors.

There are 4 ways to deal with invalid files as specified by the --invalid-file-handeling flag.

stub (default) replaces the references to the invalid files with an empty stub file.
This will show up as an empty file inside the snapshot indicating there was a file
here but it can no longer be accessed.

keep - does nothing to the found invalid files.

fail - gives and error on found invalid files

remove - completely deletes the references to the invalid files and removes them
from the snapshot.
	`
)

func (c *commandSnapshotFixInvalidFiles) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("invalid-files", fixInvalidFilesHelp)
	c.common.setup(svc, cmd)

	cmd.Flag("invalid-file-handling", "How to handle invalid files").Default(invalidEntryStub).EnumVar(&c.invalidFileHandling, invalidEntryFail, invalidEntryStub, invalidEntryKeep, invalidEntryRemove)
	cmd.Flag("verify-files-percent", "Verify a percentage of files by fully downloading them [0.0 .. 100.0]").Default("0").Float64Var(&c.verifyFilesPercent)

	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandSnapshotFixInvalidFiles) rewriteEntry(ctx context.Context, dirRelativePath string, ent *snapshot.DirEntry) (*snapshot.DirEntry, error) {
	fname := dirRelativePath + "/" + ent.Name

	if ent.Type != snapshot.EntryTypeDirectory {
		if err := c.verifier.VerifyFile(ctx, ent.ObjectID, fname); err != nil {
			log(ctx).Warnf("removing invalid file %v due to: %v", fname, err)

			return c.failedFileCallback(ctx, dirRelativePath, ent, err)
		}
	}

	return ent, nil
}

func (c *commandSnapshotFixInvalidFiles) run(ctx context.Context, rep repo.RepositoryWriter) error {
	c.failedFileCallback = failedEntryCallback(rep, c.invalidFileHandling)

	opts := snapshotfs.VerifierOptions{
		VerifyFilesPercent: c.verifyFilesPercent,
	}

	if dr, ok := rep.(repo.DirectRepository); ok {
		blobMap, err := blob.ReadBlobMap(ctx, dr.BlobReader())
		if err != nil {
			return errors.Wrap(err, "unable to read blob map")
		}

		opts.BlobMap = blobMap
	}

	c.verifier = snapshotfs.NewVerifier(ctx, rep, opts)

	return c.common.rewriteMatchingSnapshots(ctx, rep, c.rewriteEntry)
}
