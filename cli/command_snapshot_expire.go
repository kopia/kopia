package cli

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

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

type cutoffTimes struct {
	annual  time.Time
	monthly time.Time
	daily   time.Time
	hourly  time.Time
	weekly  time.Time
}

func yearsAgo(base time.Time, n int) time.Time {
	return base.AddDate(-n, 0, 0)
}

func monthsAgo(base time.Time, n int) time.Time {
	return base.AddDate(0, -n, 0)
}

func daysAgo(base time.Time, n int) time.Time {
	return base.AddDate(0, 0, -n)
}

func weeksAgo(base time.Time, n int) time.Time {
	return base.AddDate(0, 0, -n*7)
}

func hoursAgo(base time.Time, n int) time.Time {
	return base.Add(time.Duration(-n) * time.Hour)
}

func expireSnapshotsForSingleSource(snapshots []*snapshot.Manifest, src snapshot.SourceInfo, pol *snapshot.Policy, snapshotNames []string) []string {
	var toDelete []string

	now := time.Now()
	maxTime := now.Add(365 * 24 * time.Hour)

	cutoffTime := func(setting *int, add func(time.Time, int) time.Time) time.Time {
		if setting != nil {
			return add(now, *setting)
		}

		return maxTime
	}

	cutoff := cutoffTimes{
		annual:  cutoffTime(pol.RetentionPolicy.KeepAnnual, yearsAgo),
		monthly: cutoffTime(pol.RetentionPolicy.KeepMonthly, monthsAgo),
		daily:   cutoffTime(pol.RetentionPolicy.KeepDaily, daysAgo),
		hourly:  cutoffTime(pol.RetentionPolicy.KeepHourly, hoursAgo),
		weekly:  cutoffTime(pol.RetentionPolicy.KeepHourly, weeksAgo),
	}

	fmt.Printf("\nProcessing %v\n", src)
	ids := make(map[string]bool)
	idCounters := make(map[string]int)

	for i, s := range snapshots {
		keep := getReasonsToKeep(i, s, cutoff, pol, ids, idCounters)

		tm := s.StartTime.Local().Format("2006-01-02 15:04:05 MST")
		if len(keep) > 0 {
			fmt.Printf("  keeping  %v (%v) %v\n", tm, s.ID, strings.Join(keep, ","))
		} else {
			fmt.Printf("  deleting %v (%v)\n", tm, s.ID)
			toDelete = append(toDelete, s.ID)
		}
	}

	return toDelete
}

func getReasonsToKeep(i int, s *snapshot.Manifest, cutoff cutoffTimes, pol *snapshot.Policy, ids map[string]bool, idCounters map[string]int) []string {
	if s.IncompleteReason != "" {
		return nil
	}

	var keepReasons []string
	var zeroTime time.Time

	yyyy, wk := s.StartTime.ISOWeek()

	cases := []struct {
		cutoffTime     time.Time
		timePeriodID   string
		timePeriodType string
		max            *int
	}{
		{zeroTime, fmt.Sprintf("%v", i), "latest", pol.RetentionPolicy.KeepLatest},
		{cutoff.annual, s.StartTime.Format("2006"), "annual", pol.RetentionPolicy.KeepAnnual},
		{cutoff.monthly, s.StartTime.Format("2006-01"), "monthly", pol.RetentionPolicy.KeepMonthly},
		{cutoff.weekly, fmt.Sprintf("%04v-%02v", yyyy, wk), "weekly", pol.RetentionPolicy.KeepWeekly},
		{cutoff.daily, s.StartTime.Format("2006-01-02"), "daily", pol.RetentionPolicy.KeepDaily},
		{cutoff.hourly, s.StartTime.Format("2006-01-02 15"), "hourly", pol.RetentionPolicy.KeepHourly},
	}

	for _, c := range cases {
		if c.max == nil {
			continue
		}
		if s.StartTime.Before(c.cutoffTime) {
			continue
		}

		if _, exists := ids[c.timePeriodID]; exists {
			continue
		}

		if idCounters[c.timePeriodType] < *c.max {
			ids[c.timePeriodID] = true
			idCounters[c.timePeriodType]++
			keepReasons = append(keepReasons, c.timePeriodType)
		}
	}

	return keepReasons
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

func expireSnapshots(pmgr *snapshot.PolicyManager, snapshots []*snapshot.Manifest, names []string) ([]string, error) {
	var lastSource snapshot.SourceInfo
	var pendingSnapshots []*snapshot.Manifest
	var pendingNames []string
	var toDelete []string

	flush := func() error {
		if len(pendingSnapshots) > 0 {
			src := pendingSnapshots[0].Source
			pol, err := pmgr.GetEffectivePolicy(src)
			if err != nil {
				return err
			}
			td := expireSnapshotsForSingleSource(pendingSnapshots, src, pol, pendingNames)
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

	sort.Slice(snapshots, func(i, j int) bool {
		s1, s2 := snapshots[i].Source, snapshots[j].Source

		if s1.String() != s2.String() {
			return s1.String() < s2.String()
		}

		return snapshots[i].StartTime.Before(snapshots[j].StartTime)
	})

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

func runExpireCommand(ctx context.Context, rep *repo.Repository) error {
	mgr := snapshot.NewManager(rep)
	pmgr := snapshot.NewPolicyManager(rep)
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
	snapshotExpireCommand.Action(repositoryAction(runExpireCommand))
}
