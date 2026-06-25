package cli

import (
	"context"
	"path"
	"slices"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs/ignorefs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

type commandSnapshotFixRemoveFiles struct {
	common commonRewriteSnapshots

	removeObjectIDs   []string
	removeFilesByName []string
	removeIgnoredFiles bool
}

func (c *commandSnapshotFixRemoveFiles) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("remove-files", "Remove references to the specified files from snapshots.")
	c.common.setup(svc, cmd)

	cmd.Flag("object-id", "Remove files by their object ID").StringsVar(&c.removeObjectIDs)
	cmd.Flag("filename", "Remove files by filename (wildcards are supported)").StringsVar(&c.removeFilesByName)
	cmd.Flag("ignored-files", "Remove files that match current ignore rules (.kopiaignore and policy); requires live access to the snapshot source path on this machine").BoolVar(&c.removeIgnoredFiles)

	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandSnapshotFixRemoveFiles) shouldRemoveBySelector(pathFromRoot string, ent *snapshot.DirEntry) (bool, error) {
	if slices.Contains(c.removeObjectIDs, ent.ObjectID.String()) {
		return true, nil
	}

	for _, n := range c.removeFilesByName {
		matched, err := path.Match(n, ent.Name)
		if err != nil {
			return false, errors.Wrap(err, "invalid wildcard")
		}

		if matched {
			return true, nil
		}
	}

	return false, nil
}

func (c *commandSnapshotFixRemoveFiles) rewriteEntryFactory(rep repo.RepositoryWriter) RewriteEntryFactory {
	return func(ctx context.Context, source snapshot.SourceInfo, policyTree *policy.Tree) (snapshotfs.RewriteDirEntryCallback, error) {
		var checker *ignorefs.Checker

		if c.removeIgnoredFiles {
			if !isLocalSnapshotSource(source, rep) {
				return nil, errors.Errorf("source %v is not local; --ignored-files requires access to the live source path on this machine", source)
			}

			dir, err := localfs.Directory(source.Path)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to open live source directory %v", source.Path)
			}

			checker = ignorefs.NewChecker(dir, policyTree)
		}

		return func(ctx context.Context, pathFromRoot string, ent *snapshot.DirEntry) (*snapshot.DirEntry, error) {
			remove, err := c.shouldRemoveBySelector(pathFromRoot, ent)
			if err != nil {
				return nil, err
			}

			if remove {
				log(ctx).Infof("will remove file %v", pathFromRoot)

				return nil, nil
			}

			if checker != nil {
				isDir := ent.Type == snapshot.EntryTypeDirectory

				ignored, err := checker.IsIgnored(ctx, pathFromRoot, isDir)
				if err != nil {
					return nil, errors.Wrapf(err, "unable to evaluate ignore rules for %v", pathFromRoot)
				}

				if ignored {
					log(ctx).Infof("will remove ignored file %v", pathFromRoot)

					return nil, nil
				}
			}

			return ent, nil
		}, nil
	}
}

func isLocalSnapshotSource(source snapshot.SourceInfo, rep repo.Repository) bool {
	return source.Host == rep.ClientOptions().Hostname &&
		source.UserName == rep.ClientOptions().Username &&
		source.Path != ""
}

func (c *commandSnapshotFixRemoveFiles) run(ctx context.Context, rep repo.RepositoryWriter) error {
	if len(c.removeObjectIDs)+len(c.removeFilesByName) == 0 && !c.removeIgnoredFiles {
		return errors.New("must specify files to remove")
	}

	return c.common.rewriteMatchingSnapshotsWithFactory(ctx, rep, c.rewriteEntryFactory(rep))
}
