package cli

import (
	"context"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

var (
	snapshotExpireCommand = snapshotCommands.Command("expire", "Remove old snapshots according to defined expiration policies.")

	snapshotExpireAll    = snapshotExpireCommand.Flag("all", "Expire all snapshots").Bool()
	snapshotExpirePaths  = snapshotExpireCommand.Arg("path", "Expire snapshots for given paths only").Strings()
	snapshotExpireDelete = snapshotExpireCommand.Flag("delete", "Whether to actually delete snapshots").Bool()
)

func getSnapshotSourcesToExpire(ctx context.Context, rep repo.Repository) ([]snapshot.SourceInfo, error) {
	if *snapshotExpireAll {
		return snapshot.ListSources(ctx, rep)
	}

	var result []snapshot.SourceInfo

	for _, p := range *snapshotExpirePaths {
		src, err := snapshot.ParseSourceInfo(p, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to parse %q", p)
		}

		result = append(result, src)
	}

	return result, nil
}

func runExpireCommand(ctx context.Context, rep repo.Repository) error {
	sources, err := getSnapshotSourcesToExpire(ctx, rep)
	if err != nil {
		return err
	}

	sort.Slice(sources, func(i, j int) bool {
		return sources[i].String() < sources[j].String()
	})

	for _, src := range sources {
		deleted, err := policy.ApplyRetentionPolicy(ctx, rep, src, *snapshotExpireDelete)
		if err != nil {
			return errors.Wrapf(err, "error applying retention policy to %v", src)
		}

		if len(deleted) == 0 {
			log(ctx).Infof("Nothing to delete for %v.", src)
			continue
		}

		if *snapshotExpireDelete {
			log(ctx).Infof("Deleted %v snapshots of %v...", len(deleted), src)
		} else {
			log(ctx).Infof("%v snapshot(s) of %v would be deleted. Pass --delete to do it.", len(deleted), src)

			for _, it := range deleted {
				log(ctx).Infof("  %v", formatTimestamp(it.StartTime))
			}
		}
	}

	return nil
}

func init() {
	snapshotExpireCommand.Action(repositoryAction(runExpireCommand))
}
