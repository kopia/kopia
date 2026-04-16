package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
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
	manifestIDs        []string
	sources            []string
	commit             bool
	parallel           int
	invalidDirHandling string
}

const (
	invalidEntryKeep   = "keep"   // keep unreadable file/directory
	invalidEntryStub   = "stub"   // replaces unreadable file/directory with a stub file
	invalidEntryFail   = "fail"   // fail the command
	invalidEntryRemove = "remove" // removes unreadable file/directory
)

func (c *commonRewriteSnapshots) setup(svc appServices, cmd *kingpin.CmdClause) {
	_ = svc

	cmd.Flag("manifest-id", "Manifest IDs").StringsVar(&c.manifestIDs)
	cmd.Flag("source", "Source to target (username@hostname:/path)").StringsVar(&c.sources)
	cmd.Flag("commit", "Update snapshot manifests").BoolVar(&c.commit)
	cmd.Flag("parallel", "Parallelism").IntVar(&c.parallel)
	cmd.Flag("invalid-directory-handling", "Handling of invalid directories").Default(invalidEntryStub).EnumVar(&c.invalidDirHandling, invalidEntryFail, invalidEntryStub, invalidEntryKeep)
}

func failedEntryCallback(rep repo.RepositoryWriter, enumVal string) snapshotfs.RewriteFailedEntryCallback {
	switch enumVal {
	default:
		return snapshotfs.RewriteFail
	case invalidEntryStub:
		return snapshotfs.RewriteAsStub(rep)
	case invalidEntryRemove:
		return snapshotfs.RewriteRemove
	case invalidEntryKeep:
		return snapshotfs.RewriteKeep
	}
}

func (c *commonRewriteSnapshots) rewriteMatchingSnapshots(ctx context.Context, rep repo.RepositoryWriter, rewrite snapshotfs.RewriteDirEntryCallback) error {
	rw, err := snapshotfs.NewDirRewriter(ctx, rep, snapshotfs.DirRewriterOptions{
		Parallel:               c.parallel,
		RewriteEntry:           rewrite,
		OnDirectoryReadFailure: failedEntryCallback(rep, c.invalidDirHandling),
	})
	if err != nil {
		return errors.Wrap(err, "unable to create dir rewriter")
	}

	defer rw.Close(ctx)

	var updatedSnapshots int

	manifestIDs, err := c.listManifestIDs(ctx, rep)
	if err != nil {
		return err
	}

	manifests, err := snapshot.LoadSnapshots(ctx, rep, manifestIDs)
	if err != nil {
		return errors.Wrap(err, "error loading snapshots")
	}

	for _, mg := range snapshot.GroupBySource(manifests) {
		log(ctx).Infof("Processing snapshot %v", mg[0].Source)

		policyTree, err := policy.TreeForSource(ctx, rep, mg[0].Source)
		if err != nil {
			return errors.Wrap(err, "unable to get policy tree")
		}

		metadataComp := policyTree.EffectivePolicy().MetadataCompressionPolicy.MetadataCompressor()

		for _, man := range snapshot.SortByTime(mg, false) {
			log(ctx).Debugf("  %v (%v)", formatTimestamp(man.StartTime.ToTime()), man.ID)

			old := man.Clone()

			changed, err := rw.RewriteSnapshotManifest(ctx, man, metadataComp)
			if err != nil {
				return errors.Wrap(err, "error rewriting manifest")
			}

			if !changed {
				log(ctx).Infof("  %v unchanged (%v)", formatTimestamp(man.StartTime.ToTime()), man.ID)

				continue
			}

			if c.commit {
				if err := snapshot.UpdateSnapshot(ctx, rep, man); err != nil {
					return errors.Wrap(err, "error updating snapshot")
				}
			}

			log(ctx).Infof("  %v replaced manifest from %v to %v", formatTimestamp(man.StartTime.ToTime()), old.ID, man.ID)
			log(ctx).Infof("    diff %v %v", old.RootEntry.ObjectID, man.RootEntry.ObjectID)

			if d := snapshotSizeDelta(old, man); d != "" {
				log(ctx).Infof("    delta:%v", d)
			}

			updatedSnapshots++
		}
	}

	if updatedSnapshots > 0 {
		if !c.commit {
			log(ctx).Infof("Fixed %v snapshots, but snapshot manifests were not updated. Pass --commit to update snapshots.", updatedSnapshots)
		} else {
			log(ctx).Infof("Fixed and committed %v snapshots.", updatedSnapshots)
		}
	}

	if updatedSnapshots == 0 {
		log(ctx).Info("No changes.")
	}

	return nil
}

func snapshotSizeDelta(m1, m2 *snapshot.Manifest) string {
	if m1.RootEntry == nil || m2.RootEntry == nil {
		return ""
	}

	if m1.RootEntry.DirSummary == nil || m2.RootEntry.DirSummary == nil {
		return ""
	}

	deltaBytes := m2.RootEntry.DirSummary.TotalFileSize - m1.RootEntry.DirSummary.TotalFileSize
	if deltaBytes < 0 {
		return "-" + units.BytesString(-deltaBytes)
	}

	if deltaBytes > 0 {
		return "+" + units.BytesString(deltaBytes)
	}

	return ""
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
		log(ctx).Info("Listing all snapshots...")

		m, err := snapshot.ListSnapshotManifests(ctx, rep, nil, nil)
		if err != nil {
			return nil, errors.Wrap(err, "unable to list snapshot manifests")
		}

		manifests = append(manifests, m...)
	}

	return manifests, nil
}
