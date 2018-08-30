package cli

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/policy"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

var (
	snapshotExpireCommand = snapshotCommands.Command("expire", "Remove old snapshots according to defined expiration policies.")

	snapshotExpireHost   = snapshotExpireCommand.Flag("host", "Expire snapshots from a given host").Default("").String()
	snapshotExpireUser   = snapshotExpireCommand.Flag("user", "Expire snapshots from a given user").Default("").String()
	snapshotExpireAll    = snapshotExpireCommand.Flag("all", "Expire all snapshots").Bool()
	snapshotExpirePaths  = snapshotExpireCommand.Arg("path", "Expire snapshots for a given paths only").Strings()
	snapshotExpireDelete = snapshotExpireCommand.Flag("delete", "Whether to actually delete snapshots").Default("no").String()
)

func getSnapshotNamesToExpire(mgr *snapshot.Manager) ([]string, error) {
	if !*snapshotExpireAll && len(*snapshotExpirePaths) == 0 {
		return nil, fmt.Errorf("Must specify paths to expire or --all")
	}

	if *snapshotExpireAll {
		printStderr("Scanning all active snapshots...\n")
		return mgr.ListSnapshotManifests(nil), nil
	}

	var result []string

	for _, p := range *snapshotExpirePaths {
		src, err := snapshot.ParseSourceInfo(p, getHostName(), getUserName())
		if err != nil {
			return nil, fmt.Errorf("unable to parse %v: %v", p, err)
		}

		log.Debugf("Looking for snapshots of %v", src)

		matches := mgr.ListSnapshotManifests(&src)
		if err != nil {
			return nil, fmt.Errorf("error listing snapshots for %v: %v", src, err)
		}

		log.Debugf("Found %v snapshots of %v", len(matches), src)

		result = append(result, matches...)
	}

	return result, nil
}

func runExpireCommand(ctx context.Context, rep *repo.Repository) error {
	mgr := snapshot.NewManager(rep)
	pmgr := policy.NewPolicyManager(rep)
	snapshotNames, err := getSnapshotNamesToExpire(mgr)
	if err != nil {
		return err
	}

	snapshots, err := mgr.LoadSnapshots(snapshotNames)
	if err != nil {
		return err
	}
	snapshots = filterHostAndUser(snapshots)
	toDelete, err := policy.GetExpiredSnapshots(pmgr, snapshots)
	if err != nil {
		return err
	}

	for _, snapshots := range snapshot.GroupBySource(toDelete) {
		src := snapshots[0].Source
		fmt.Printf("Would delete %v snapshots for %v\n", len(snapshots), src)
	}

	printStderr("\n*** ")

	if len(toDelete) == 0 {
		printStderr("Nothing to delete.\n")
		return nil
	}
	if *snapshotExpireDelete == "yes" {
		printStderr("Deleting %v snapshots...\n", len(toDelete))
		for _, it := range toDelete {
			rep.Manifests.Delete(it.ID)
		}
	} else {
		printStderr("%v snapshot(s) would be deleted. Pass --delete=yes to do it.\n", len(toDelete))
	}

	return nil
}

func filterHostAndUser(snapshots []*snapshot.Manifest) []*snapshot.Manifest {
	if *snapshotExpireHost == "" && *snapshotExpireUser == "" {
		return snapshots
	}

	var result []*snapshot.Manifest

	for _, s := range snapshots {
		if *snapshotExpireHost != "" && *snapshotExpireHost != s.Source.Host {
			continue
		}

		if *snapshotExpireUser != "" && *snapshotExpireUser != s.Source.UserName {
			continue
		}

		result = append(result, s)
	}

	return result
}

func init() {
	snapshotExpireCommand.Action(repositoryAction(runExpireCommand))
}
