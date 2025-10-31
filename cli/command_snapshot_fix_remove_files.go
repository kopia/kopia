package cli

import (
	"context"
	"path"
	"slices"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

type commandSnapshotFixRemoveFiles struct {
	common commonRewriteSnapshots

	removeObjectIDs []string
	// List of patterns to match against filename
	removeFilesByName []string
	// List of patterns to match against full file path
	removeFilesByPath []string
}

func (c *commandSnapshotFixRemoveFiles) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("remove-files", "Remove references to the specified files from snapshots.")
	c.common.setup(svc, cmd)

	cmd.Flag("object-id", "Remove files by their object ID").StringsVar(&c.removeObjectIDs)
	cmd.Flag("filename", "Remove files by filename (wildcards are supported)").StringsVar(&c.removeFilesByName)
	cmd.Flag("path", "Remove files by path relative to snapshot root (wildcards are supported; must match full path)").StringsVar(&c.removeFilesByPath)

	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandSnapshotFixRemoveFiles) rewriteEntry(ctx context.Context, pathFromRoot string, ent *snapshot.DirEntry) (*snapshot.DirEntry, error) {
	if slices.Contains(c.removeObjectIDs, ent.ObjectID.String()) {
		log(ctx).Infof("will remove file %v", pathFromRoot)

		return nil, nil
	}

	for _, n := range c.removeFilesByName {
		matched, err := path.Match(n, ent.Name)
		if err != nil {
			return nil, errors.Wrap(err, "invalid wildcard")
		}

		if matched {
			log(ctx).Infof("will remove file %v", pathFromRoot)

			return nil, nil
		}
	}

	for _, pattern := range c.removeFilesByPath {
		matched, err := path.Match(pattern, pathFromRoot)
		if err != nil {
			return nil, errors.Wrap(err, "invalid wildcard")
		}

		if matched {
			log(ctx).Infof("will remove file %v", pathFromRoot)
			return nil, nil
		}
	}

	return ent, nil
}

func (c *commandSnapshotFixRemoveFiles) run(ctx context.Context, rep repo.RepositoryWriter) error {
	if len(c.removeObjectIDs)+len(c.removeFilesByName)+len(c.removeFilesByPath) == 0 {
		return errors.New("must specify files to remove")
	}

	return c.common.rewriteMatchingSnapshots(ctx, rep, c.rewriteEntry)
}
