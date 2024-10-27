package cli

import (
	"context"
	"path"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

type commandSnapshotFixRemoveFiles struct {
	common commonRewriteSnapshots

	removeObjectIDs   []string
	removeFilesByName []string
}

func (c *commandSnapshotFixRemoveFiles) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("remove-files", "Remove references to the specified files from snapshots.")
	c.common.setup(svc, cmd)

	cmd.Flag("object-id", "Remove files by their object ID").StringsVar(&c.removeObjectIDs)
	cmd.Flag("filename", "Remove files by filename (wildcards are supported)").StringsVar(&c.removeFilesByName)

	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandSnapshotFixRemoveFiles) rewriteEntry(ctx context.Context, dirRelativePath string, ent *snapshot.DirEntry) (*snapshot.DirEntry, error) {
	for _, id := range c.removeObjectIDs {
		if ent.ObjectID.String() == id {
			log(ctx).Infof("will remove file %v", path.Join(dirRelativePath, ent.Name))

			return nil, nil
		}
	}

	for _, n := range c.removeFilesByName {
		matched, err := path.Match(n, ent.Name)
		if err != nil {
			return nil, errors.Wrap(err, "invalid wildcard")
		}

		if matched {
			log(ctx).Infof("will remove file %v", path.Join(dirRelativePath, ent.Name))

			return nil, nil
		}
	}

	return ent, nil
}

func (c *commandSnapshotFixRemoveFiles) run(ctx context.Context, rep repo.RepositoryWriter) error {
	if len(c.removeObjectIDs)+len(c.removeFilesByName) == 0 {
		return errors.New("must specify files to remove")
	}

	return c.common.rewriteMatchingSnapshots(ctx, rep, c.rewriteEntry)
}
