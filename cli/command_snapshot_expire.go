package cli

import (
	"context"
	"sort"

	"github.com/kopia/kopia/policy"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

var (
	snapshotExpireCommand = snapshotCommands.Command("expire", "Remove old snapshots according to defined expiration policies.")

	snapshotExpireAll    = snapshotExpireCommand.Flag("all", "Expire all snapshots").Bool()
	snapshotExpirePaths  = snapshotExpireCommand.Arg("path", "Expire snapshots for a given paths only").Strings()
	snapshotExpireDelete = snapshotExpireCommand.Flag("delete", "Whether to actually delete snapshots").Default("no").String()
)

func getSnapshotSourcesToExpire(ctx context.Context, rep *repo.Repository) ([]snapshot.SourceInfo, error) {
	if *snapshotExpireAll {
		return snapshot.ListSources(ctx, rep)
	}

	var result []snapshot.SourceInfo
	for _, p := range *snapshotExpirePaths {
		src, err := snapshot.ParseSourceInfo(p, getHostName(), getUserName())
		if err != nil {
			return nil, err
		}
		result = append(result, src)
	}

	return result, nil
}

func runExpireCommand(ctx context.Context, rep *repo.Repository) error {
	sources, err := getSnapshotSourcesToExpire(ctx, rep)
	if err != nil {
		return err
	}

	sort.Slice(sources, func(i, j int) bool {
		return sources[i].String() < sources[j].String()
	})

	for _, src := range sources {
		snapshots, err := snapshot.ListSnapshots(ctx, rep, src)
		if err != nil {
			return err
		}

		toDelete, err := policy.GetExpiredSnapshots(ctx, rep, snapshots)
		if err != nil {
			return err
		}

		if len(toDelete) == 0 {
			printStderr("Nothing to delete for %v.\n", src)
			continue
		}
		if *snapshotExpireDelete == "yes" {
			printStderr("Deleting %v snapshots of %v...\n", len(toDelete), src)
			for _, it := range toDelete {
				if err := rep.Manifests.Delete(ctx, it.ID); err != nil {
					return err
				}
			}
		} else {
			printStderr("%v snapshot(s) of %v would be deleted. Pass --delete=yes to do it.\n", len(toDelete), src)
			for _, it := range toDelete {
				printStderr("  %v\n", it.StartTime.Format(timeFormat))
			}
		}
	}

	return nil
}

func init() {
	snapshotExpireCommand.Action(repositoryAction(runExpireCommand))
}
