package cli

import (
	"context"
	"sort"

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

func getSnapshotSourcesToExpire(ctx context.Context, rep *repo.Repository) ([]snapshot.SourceInfo, error) {
	if *snapshotExpireAll {
		return snapshot.ListSources(ctx, rep)
	}

	var result []snapshot.SourceInfo

	for _, p := range *snapshotExpirePaths {
		src, err := snapshot.ParseSourceInfo(p, rep.Hostname, rep.Username)
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
		deleted, err := policy.ApplyRetentionPolicy(ctx, rep, src, *snapshotExpireDelete)
		if err != nil {
			return err
		}

		if len(deleted) == 0 {
			printStderr("Nothing to delete for %v.\n", src)
			continue
		}

		if *snapshotExpireDelete {
			printStderr("Deleted %v snapshots of %v...\n", len(deleted), src)
		} else {
			printStderr("%v snapshot(s) of %v would be deleted. Pass --delete to do it.\n", len(deleted), src)

			for _, it := range deleted {
				printStderr("  %v\n", formatTimestamp(it.StartTime))
			}
		}
	}

	return nil
}

func init() {
	snapshotExpireCommand.Action(repositoryAction(runExpireCommand))
}
