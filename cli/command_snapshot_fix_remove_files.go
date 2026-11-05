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

	removeObjectIDs   []string
	removeFilesByName []string
	removeFilesByPath []string
}

func (c *commandSnapshotFixRemoveFiles) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("remove-files", "Remove references to the specified files from snapshots.")
	c.common.setup(svc, cmd)

	cmd.Flag("object-id", "Remove files by their object ID").StringsVar(&c.removeObjectIDs)
	cmd.Flag("filename", "Remove files by filename (wildcards are supported)").StringsVar(&c.removeFilesByName)
	cmd.Flag("path", "Remove files by path relative to the snapshot root (wildcards are supported)").StringsVar(&c.removeFilesByPath)

	cmd.Action(svc.repositoryWriterActionWithMaintenance(c.run))
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

	// the root's path is ".", which "*" matches; path wildcards apply to the entries below it
	if pathFromRoot == "." {
		return ent, nil
	}

	for _, p := range c.removeFilesByPath {
		matched, err := path.Match(p, pathFromRoot)
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

	if err := validateWildcards(c.removeFilesByName); err != nil {
		return err
	}

	if err := validateWildcards(c.removeFilesByPath); err != nil {
		return err
	}

	return c.common.rewriteMatchingSnapshots(ctx, rep, c.rewriteEntry)
}

// validateWildcards fails on a malformed pattern before any snapshot is read,
// since rewriting only reports it when an entry is matched against it.
func validateWildcards(patterns []string) error {
	for _, p := range patterns {
		if _, err := path.Match(p, ""); err != nil {
			return errors.Wrapf(err, "invalid wildcard %q", p)
		}
	}

	return nil
}
