package policy

import (
	"context"
	"strings"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

// ApplyRetentionPolicy applies retention policy to a given source by deleting expired snapshots.
func ApplyRetentionPolicy(ctx context.Context, rep repo.Repository, sourceInfo snapshot.SourceInfo, reallyDelete bool) ([]*snapshot.Manifest, error) {
	snapshots, err := snapshot.ListSnapshots(ctx, rep, sourceInfo)
	if err != nil {
		return nil, err
	}

	toDelete, err := getExpiredSnapshots(ctx, rep, snapshots)
	if err != nil {
		return nil, err
	}

	if reallyDelete {
		for _, it := range toDelete {
			if err := rep.DeleteManifest(ctx, it.ID); err != nil {
				return toDelete, err
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

	pol, _, err := GetEffectivePolicy(ctx, rep, src)
	if err != nil {
		return nil, err
	}

	pol.RetentionPolicy.ComputeRetentionReasons(snapshots)

	var toDelete []*snapshot.Manifest

	for _, s := range snapshots {
		if len(s.RetentionReasons) == 0 {
			log(ctx).Debugf("  deleting %v", s.StartTime)
			toDelete = append(toDelete, s)
		} else {
			log(ctx).Debugf("  keeping %v reasons: [%v]", s.StartTime, strings.Join(s.RetentionReasons, ","))
		}
	}

	return toDelete, nil
}
