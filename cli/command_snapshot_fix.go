package cli

import (
	"context"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

type commandSnapshotFix struct {
	invalidFiles commandSnapshotFixInvalidFiles
	removeFiles  commandSnapshotFixRemoveFiles
}

func (c *commandSnapshotFix) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("fix", "Commands to fix snapshot consistency issues")

	c.invalidFiles.setup(svc, cmd)
	c.removeFiles.setup(svc, cmd)
}

type commonRewriteSnapshots struct {
	manifestIDs []string
	sources     []string
	commit      bool
}

func (c *commonRewriteSnapshots) setup(svc appServices, cmd *kingpin.CmdClause) {
	_ = svc

	cmd.Flag("manifest-id", "Manifest IDs").StringsVar(&c.manifestIDs)
	cmd.Flag("source", "Source to target (username@hostname:/path)").StringsVar(&c.sources)
	cmd.Flag("commit", "Update snapshot manifests").BoolVar(&c.commit)
}

func (c *commonRewriteSnapshots) rewriteMatchingSnapshots(ctx context.Context, rep repo.RepositoryWriter, rewrite snapshotfs.DirRewriterCallback) error {
	rw := snapshotfs.NewDirRewriter(rep, rewrite)

	var fixed bool

	manifests, err := c.listManifestIDs(ctx, rep)
	if err != nil {
		return err
	}

	for _, manID := range manifests {
		man, err := snapshot.LoadSnapshot(ctx, rep, manID)
		if err != nil {
			return errors.Wrapf(err, "error loading manifest %v", manID)
		}

		old := *man

		changed, err := rw.RewriteSnapshotManifest(ctx, man)
		if err != nil {
			return errors.Wrap(err, "error rewriting manifest")
		}

		if !changed {
			log(ctx).Infof("No change to snapshot %v at %v",
				man.Source,
				formatTimestamp(man.StartTime))

			continue
		}

		if changed {
			oldManifestID := man.ID

			if err := snapshot.UpdateSnapshot(ctx, rep, man); err != nil {
				return errors.Wrap(err, "error updating snapshot")
			}

			log(ctx).Infof("Fixing snapshot %v at %v.\n  Manifest ID: %v => %v\n  Root: %v => %v",
				man.Source,
				formatTimestamp(man.StartTime),
				oldManifestID,
				man.ID,
				old.RootEntry.ObjectID,
				man.RootEntry.ObjectID)

			fixed = true
		}
	}

	if fixed && !c.commit {
		return errors.Errorf("fixes made, but not committed, pass --commit to update snapshots")
	}

	return nil
}

func (c *commonRewriteSnapshots) listManifestIDs(ctx context.Context, rep repo.Repository) ([]manifest.ID, error) {
	manifests := toManifestIDs(c.manifestIDs)

	for _, src := range c.sources {
		log(ctx).Infof("Listing snapshots for source %q...", src)

		si, err := snapshot.ParseSourceInfo(src, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse source")
		}

		m, err := snapshot.ListSnapshotManifests(ctx, rep, &si, nil)
		if err != nil {
			return nil, errors.Wrap(err, "unable to list manifests")
		}

		if len(m) == 0 {
			return nil, errors.Errorf("no snapshots for %v", src)
		}

		manifests = append(manifests, m...)
	}

	if len(manifests) == 0 {
		log(ctx).Infof("Listing all snapshots...")

		m, err := snapshot.ListSnapshotManifests(ctx, rep, nil, nil)
		if err != nil {
			return nil, errors.Wrap(err, "unable to list snapshot manifests")
		}

		manifests = append(manifests, m...)
	}

	return manifests, nil
}
