package cli

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/kopia/kopia/policy"
	"github.com/kopia/kopia/snapshot"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	snapshotExpireCommand = snapshotCommands.Command("expire", "Remove old snapshots according to defined expiration policies.")

	snapshotExpireHost   = snapshotExpireCommand.Flag("host", "Expire snapshots from a given host").Default("").String()
	snapshotExpireUser   = snapshotExpireCommand.Flag("user", "Expire snapshots from a given user").Default("").String()
	snapshotExpireAll    = snapshotExpireCommand.Flag("all", "Expire all snapshots").Bool()
	snapshotExpirePaths  = snapshotExpireCommand.Arg("path", "Expire snapshots for a given paths only").Strings()
	snapshotExpireDelete = snapshotExpireCommand.Flag("delete", "Whether to actually delete snapshots").Default("no").String()
)

func expireSnapshotsForSingleSource(snapshots []*snapshot.Manifest, src *snapshot.SourceInfo, pol *policy.Policy, snapshotNames []string) []string {
	var toDelete []string

	ids := make(map[string]bool)
	idCounters := make(map[string]int)

	var annualCutoffTime time.Time
	var monthlyCutoffTime time.Time
	var dailyCutoffTime time.Time
	var hourlyCutoffTime time.Time
	var weeklyCutoffTime time.Time

	if pol.RetentionPolicy.KeepAnnual != nil {
		annualCutoffTime = time.Now().AddDate(-*pol.RetentionPolicy.KeepAnnual, 0, 0)
	}

	if pol.RetentionPolicy.KeepMonthly != nil {
		monthlyCutoffTime = time.Now().AddDate(0, -*pol.RetentionPolicy.KeepMonthly, 0)
	}

	if pol.RetentionPolicy.KeepDaily != nil {
		dailyCutoffTime = time.Now().AddDate(0, 0, -*pol.RetentionPolicy.KeepDaily)
	}

	if pol.RetentionPolicy.KeepHourly != nil {
		hourlyCutoffTime = time.Now().Add(time.Duration(-*pol.RetentionPolicy.KeepHourly) * time.Hour)
	}

	if pol.RetentionPolicy.KeepWeekly != nil {
		weeklyCutoffTime = time.Now().AddDate(0, 0, -7**pol.RetentionPolicy.KeepWeekly)
	}

	fmt.Printf("\n%v\n", src)

	for i, s := range snapshots {
		var keep []string

		registerSnapshot := func(timePeriodID string, timePeriodType string, max int) {
			if _, exists := ids[timePeriodID]; !exists && idCounters[timePeriodType] < max {
				ids[timePeriodID] = true
				idCounters[timePeriodType]++
				keep = append(keep, timePeriodType)
			}
		}

		if s.IncompleteReason != "" {
			continue
		}

		if pol.RetentionPolicy.KeepLatest != nil {
			registerSnapshot(fmt.Sprintf("%v", i), "latest", *pol.RetentionPolicy.KeepLatest)
		}
		if s.StartTime.After(annualCutoffTime) && pol.RetentionPolicy.KeepAnnual != nil {
			registerSnapshot(s.StartTime.Format("2006"), "annual", *pol.RetentionPolicy.KeepAnnual)
		}
		if s.StartTime.After(monthlyCutoffTime) && pol.RetentionPolicy.KeepMonthly != nil {
			registerSnapshot(s.StartTime.Format("2006-01"), "monthly", *pol.RetentionPolicy.KeepMonthly)
		}
		if s.StartTime.After(weeklyCutoffTime) && pol.RetentionPolicy.KeepWeekly != nil {
			yyyy, wk := s.StartTime.ISOWeek()
			registerSnapshot(fmt.Sprintf("%04v-%02v", yyyy, wk), "weekly", *pol.RetentionPolicy.KeepWeekly)
		}
		if s.StartTime.After(dailyCutoffTime) && pol.RetentionPolicy.KeepDaily != nil {
			registerSnapshot(s.StartTime.Format("2006-01-02"), "daily", *pol.RetentionPolicy.KeepDaily)
		}
		if s.StartTime.After(hourlyCutoffTime) && pol.RetentionPolicy.KeepHourly != nil {
			registerSnapshot(s.StartTime.Format("2006-01-02 15"), "hourly", *pol.RetentionPolicy.KeepHourly)
		}

		tm := s.StartTime.Local().Format("2006-01-02 15:04:05 MST")
		if len(keep) > 0 {
			fmt.Printf("  keeping  %v %v\n", tm, strings.Join(keep, ","))
		} else {
			fmt.Printf("  deleting %v\n", tm)
			toDelete = append(toDelete, snapshotNames[i])
		}
	}

	return toDelete
}

func getSnapshotNamesToExpire(mgr *snapshot.Manager) ([]string, error) {
	if !*snapshotExpireAll && len(*snapshotExpirePaths) == 0 {
		return nil, fmt.Errorf("Must specify paths to expire or --all")
	}

	if *snapshotExpireAll {
		fmt.Fprintf(os.Stderr, "Scanning all active snapshots...\n")
		return mgr.ListSnapshotManifests(nil), nil
	}

	var result []string

	for _, p := range *snapshotExpirePaths {
		src, err := snapshot.ParseSourceInfo(p, getHostName(), getUserName())
		if err != nil {
			return nil, fmt.Errorf("unable to parse %v: %v", p, err)
		}

		log.Printf("Looking for snapshots of %v", src)

		matches := mgr.ListSnapshotManifests(&src)
		if err != nil {
			return nil, fmt.Errorf("error listing snapshots for %v: %v", src, err)
		}

		log.Printf("Found %v snapshots of %v", len(matches), src)

		result = append(result, matches...)
	}

	return result, nil
}

func expireSnapshots(pmgr *policy.Manager, snapshots []*snapshot.Manifest, names []string) ([]string, error) {
	var lastSource snapshot.SourceInfo
	var pendingSnapshots []*snapshot.Manifest
	var pendingNames []string
	var toDelete []string

	flush := func() error {
		if len(pendingSnapshots) > 0 {
			src := pendingSnapshots[0].Source
			pol, err := pmgr.GetEffectivePolicy(src.UserName, src.Host, src.Path)
			if err != nil {
				return err
			}
			td := expireSnapshotsForSingleSource(pendingSnapshots, &src, pol, pendingNames)
			if len(td) == 0 {
				fmt.Fprintf(os.Stderr, "Nothing to delete for %q.\n", src)
			} else {
				log.Printf("would delete %v out of %v snapshots for %q", len(td), len(pendingSnapshots), src)
				toDelete = append(toDelete, td...)
			}
		}
		pendingSnapshots = nil
		pendingNames = nil
		return nil
	}

	for i, s := range snapshots {
		if s.Source != lastSource {
			lastSource = s.Source
			if err := flush(); err != nil {
				return nil, err
			}
		}

		pendingSnapshots = append(pendingSnapshots, s)
		pendingNames = append(pendingNames, names[i])
	}
	if err := flush(); err != nil {
		return nil, err
	}

	return toDelete, nil
}

func runExpireCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	mgr := snapshot.NewManager(rep)
	pmgr := policy.NewManager(rep)
	snapshotNames, err := getSnapshotNamesToExpire(mgr)
	if err != nil {
		return err
	}

	snapshots, err := mgr.LoadSnapshots(snapshotNames)
	if err != nil {
		return err
	}
	snapshots = filterHostAndUser(snapshots)
	toDelete, err := expireSnapshots(pmgr, snapshots, snapshotNames)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "\n*** ")

	if len(toDelete) == 0 {
		fmt.Fprintf(os.Stderr, "Nothing to delete.\n")
		return nil
	}
	if *snapshotExpireDelete == "yes" {
		fmt.Fprintf(os.Stderr, "Deleting %v snapshots...\n", len(toDelete))
		for _, it := range toDelete {
			rep.Manifests.Delete(it)
		}
	} else {
		fmt.Fprintf(os.Stderr, "%v snapshot(s) would be deleted. Pass --delete=yes to do it.\n", len(toDelete))
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
	snapshotExpireCommand.Action(runExpireCommand)
}
