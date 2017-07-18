package cli

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/kopia/kopia/snapshot"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	expireCommand = app.Command("expire", "Remove old backups.")

	expireHost  = expireCommand.Flag("host", "Expire backups from a given host").Default("").String()
	expireUser  = expireCommand.Flag("user", "Expire backups from a given user").Default("").String()
	expireAll   = expireCommand.Flag("all", "Expire all backups").Bool()
	expirePaths = expireCommand.Arg("path", "Expire backups for a given paths only").Strings()

	expireDelete = expireCommand.Flag("delete", "Whether to actually delete backups").Default("no").String()
)

func expireSnapshotsForSingleSource(snapshots []*snapshot.Manifest, pol *snapshot.Policy, snapshotNames []string) []string {
	var toDelete []string

	ids := make(map[string]bool)
	idCounters := make(map[string]int)

	var annualCutoffTime time.Time
	var monthlyCutoffTime time.Time
	var dailyCutoffTime time.Time
	var hourlyCutoffTime time.Time
	var weeklyCutoffTime time.Time

	if pol.Expiration.KeepAnnual != nil {
		annualCutoffTime = time.Now().AddDate(-*pol.Expiration.KeepAnnual, 0, 0)
	}

	if pol.Expiration.KeepMonthly != nil {
		monthlyCutoffTime = time.Now().AddDate(0, -*pol.Expiration.KeepMonthly, 0)
	}

	if pol.Expiration.KeepDaily != nil {
		dailyCutoffTime = time.Now().AddDate(0, 0, -*pol.Expiration.KeepDaily)
	}

	if pol.Expiration.KeepHourly != nil {
		hourlyCutoffTime = time.Now().Add(time.Duration(-*pol.Expiration.KeepHourly) * time.Hour)
	}

	if pol.Expiration.KeepWeekly != nil {
		weeklyCutoffTime = time.Now().AddDate(0, 0, -7**pol.Expiration.KeepWeekly)
	}

	fmt.Printf("\n%v\n", pol.Source)

	for i, s := range snapshots {
		var keep []string

		registerSnapshot := func(timePeriodID string, timePeriodType string, max int) {
			if _, exists := ids[timePeriodID]; !exists && idCounters[timePeriodType] < max {
				ids[timePeriodID] = true
				idCounters[timePeriodType]++
				keep = append(keep, timePeriodType)
			}
		}

		if pol.Expiration.KeepLatest != nil {
			registerSnapshot(fmt.Sprintf("%v", i), "latest", *pol.Expiration.KeepLatest)
		}
		if s.StartTime.After(annualCutoffTime) && pol.Expiration.KeepAnnual != nil {
			registerSnapshot(s.StartTime.Format("2006"), "annual", *pol.Expiration.KeepAnnual)
		}
		if s.StartTime.After(monthlyCutoffTime) && pol.Expiration.KeepMonthly != nil {
			registerSnapshot(s.StartTime.Format("2006-01"), "monthly", *pol.Expiration.KeepMonthly)
		}
		if s.StartTime.After(weeklyCutoffTime) && pol.Expiration.KeepWeekly != nil {
			yyyy, wk := s.StartTime.ISOWeek()
			registerSnapshot(fmt.Sprintf("%04v-%02v", yyyy, wk), "weekly", *pol.Expiration.KeepWeekly)
		}
		if s.StartTime.After(dailyCutoffTime) && pol.Expiration.KeepDaily != nil {
			registerSnapshot(s.StartTime.Format("2006-01-02"), "daily", *pol.Expiration.KeepDaily)
		}
		if s.StartTime.After(hourlyCutoffTime) && pol.Expiration.KeepHourly != nil {
			registerSnapshot(s.StartTime.Format("2006-01-02 15"), "hourly", *pol.Expiration.KeepHourly)
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
	if !*expireAll && len(*expirePaths) == 0 {
		return nil, fmt.Errorf("Must specify paths to expire or --all")
	}

	if *expireAll {
		fmt.Fprintf(os.Stderr, "Scanning all active snapshots...\n")
		return mgr.ListSnapshotManifests(nil, -1)
	}

	var result []string

	for _, p := range *expirePaths {
		src, err := snapshot.ParseSourceInfo(p, getHostName(), getUserName())
		if err != nil {
			return nil, fmt.Errorf("unable to parse %v: %v", p, err)
		}

		log.Printf("Looking for backups of %v", src)

		matches, err := mgr.ListSnapshotManifests(&src, -1)
		if err != nil {
			return nil, fmt.Errorf("error listing backups for %v: %v", src, err)
		}

		log.Printf("Found %v backups of %v", len(matches), src)

		result = append(result, matches...)
	}

	return result, nil
}

func expireSnapshots(mgr *snapshot.Manager, snapshots []*snapshot.Manifest, names []string) ([]string, error) {
	var lastSource snapshot.SourceInfo
	var pendingSnapshots []*snapshot.Manifest
	var pendingNames []string
	var toDelete []string

	flush := func() error {
		if len(pendingSnapshots) > 0 {
			src := pendingSnapshots[0].Source
			pol, err := mgr.GetEffectivePolicy(&src)
			if err != nil {
				return err
			}
			td := expireSnapshotsForSingleSource(pendingSnapshots, pol, pendingNames)
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
	rep := mustConnectToRepository(nil)
	defer rep.Close()

	mgr := snapshot.NewManager(rep)
	snapshotNames, err := getSnapshotNamesToExpire(mgr)
	if err != nil {
		return err
	}

	snapshots, err := mgr.LoadSnapshots(snapshotNames)
	if err != nil {
		return err
	}
	snapshots = filterHostAndUser(snapshots)
	toDelete, err := expireSnapshots(mgr, snapshots, snapshotNames)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "\n*** ")

	if len(toDelete) == 0 {
		fmt.Fprintf(os.Stderr, "Nothing to delete.\n")
		return nil
	}

	if *expireDelete == "yes" {
		fmt.Fprintf(os.Stderr, "Deleting %v snapshots...\n", len(toDelete))
		if err := rep.Vault.RemoveMany(toDelete); err != nil {
			return err
		}
	} else {
		fmt.Fprintf(os.Stderr, "%v snapshot(s) would be deleted. Pass --delete=yes to do it.\n", len(toDelete))
	}

	return nil
}

func filterHostAndUser(snapshots []*snapshot.Manifest) []*snapshot.Manifest {
	if *expireHost == "" && *expireUser == "" {
		return snapshots
	}

	var result []*snapshot.Manifest

	for _, s := range snapshots {
		if *expireHost != "" && *expireHost != s.Source.Host {
			continue
		}

		if *expireUser != "" && *expireUser != s.Source.UserName {
			continue
		}

		result = append(result, s)
	}

	return result
}

func init() {
	expireCommand.Action(runExpireCommand)
}
