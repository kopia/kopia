package policy

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

// ApplyRetentionPolicy applies retention policy to a given source by deleting expired snapshots.
func ApplyRetentionPolicy(ctx context.Context, rep repo.RepositoryWriter, sourceInfo snapshot.SourceInfo, reallyDelete bool) ([]*snapshot.Manifest, error) {
	snapshots, err := snapshot.ListSnapshots(ctx, rep, sourceInfo)
	if err != nil {
		return nil, errors.Wrap(err, "error listing snapshots")
	}

	toDelete, err := getExpiredSnapshots(ctx, rep, snapshots)
	if err != nil {
		return nil, errors.Wrap(err, "unable to compute snapshots to delete")
	}

	if reallyDelete {
		for _, it := range toDelete {
			if err := rep.DeleteManifest(ctx, it.ID); err != nil {
				return toDelete, errors.Wrapf(err, "error deleting manifest %v", it.ID)
			}
		}
	}

	return toDelete, nil
}

func getExpiredSnapshots(ctx context.Context, rep repo.Repository, snapshots []*snapshot.Manifest) ([]*snapshot.Manifest, error) {
	var toDelete []*snapshot.Manifest

	for _, snapshotGroup := range snapshot.GroupBySource(snapshots) {
		td, err := getExpiredSnapshotsForSource(ctx, rep, snapshotGroup)
		if err != nil {
			return nil, err
		}

		toDelete = append(toDelete, td...)
	}

	return toDelete, nil
}

func getExpiredSnapshotsForSource(ctx context.Context, rep repo.Repository, snapshots []*snapshot.Manifest) ([]*snapshot.Manifest, error) {
	src := snapshots[0].Source

	pol, _, _, err := GetEffectivePolicy(ctx, rep, src)
	if err != nil {
		return nil, err
	}

	pol.RetentionPolicy.ComputeRetentionReasons(snapshots)

	var toDelete []*snapshot.Manifest

	for _, s := range snapshots {
		if len(s.RetentionReasons) == 0 && len(s.Pins) == 0 {
			log(ctx).Debugf("  deleting %v", s.StartTime)
			toDelete = append(toDelete, s)
		} else {
			log(ctx).Debugf("  keeping %v retention: [%v] pins: [%v]", s.StartTime, strings.Join(s.RetentionReasons, ","), strings.Join(s.Pins, ","))
		}
	}

	return toDelete, nil
}
