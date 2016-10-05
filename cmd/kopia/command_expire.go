package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/kopia/kopia/fs/repofs"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	expireCommand = app.Command("expire", "Remove old backups.")

	expireKeepLatest  = expireCommand.Flag("keep-latest", "Number of most recent backups to keep per source").Int()
	expireKeepHourly  = expireCommand.Flag("keep-hourly", "Number of most-recent hourly backups to keep per source").Int()
	expireKeepDaily   = expireCommand.Flag("keep-daily", "Number of most-recent daily backups to keep per source").Int()
	expireKeepWeekly  = expireCommand.Flag("keep-weekly", "Number of most-recent weekly backups to keep per source").Int()
	expireKeepMonthly = expireCommand.Flag("keep-monthly", "Number of most-recent monthly backups to keep per source").Int()
	expireKeepAnnual  = expireCommand.Flag("keep-annual", "Number of most-recent annual backups to keep per source").Int()

	expireDelete = expireCommand.Flag("delete", "Whether to actually delete backups").Default("no").String()
)

func expire(snapshots []*repofs.Snapshot, snapshotNames []string) []string {
	var toDelete []string

	var ids map[string]bool
	var idCounters map[string]int

	var lastSource repofs.SnapshotSourceInfo

	var annualCutoffTime time.Time
	var monthlyCutoffTime time.Time
	var dailyCutoffTime time.Time
	var hourlyCutoffTime time.Time
	var weeklyCutoffTime time.Time

	if *expireKeepAnnual > 0 {
		annualCutoffTime = time.Now().AddDate(-*expireKeepAnnual, 0, 0)
	}

	if *expireKeepMonthly > 0 {
		monthlyCutoffTime = time.Now().AddDate(0, -*expireKeepMonthly, 0)
	}

	if *expireKeepDaily > 0 {
		dailyCutoffTime = time.Now().AddDate(0, 0, -*expireKeepDaily)
	}

	if *expireKeepHourly > 0 {
		hourlyCutoffTime = time.Now().Add(time.Duration(-*expireKeepHourly) * time.Hour)
	}

	if *expireKeepWeekly > 0 {
		weeklyCutoffTime = time.Now().AddDate(0, 0, -7**expireKeepWeekly)
	}

	for i, s := range snapshots {
		if s.Source != lastSource {
			lastSource = s.Source
			ids = make(map[string]bool)
			idCounters = make(map[string]int)
			fmt.Printf("\n%v\n", s.Source)
		}

		var keep []string

		registerSnapshot := func(timePeriodID string, timePeriodType string, max int) {
			if _, exists := ids[timePeriodID]; !exists && idCounters[timePeriodType] < max {
				ids[timePeriodID] = true
				idCounters[timePeriodType]++
				keep = append(keep, timePeriodType)
			}
		}

		registerSnapshot(fmt.Sprintf("%v", i), "latest", *expireKeepLatest)
		if s.StartTime.After(annualCutoffTime) {
			registerSnapshot(s.StartTime.Format("2006"), "annual", *expireKeepAnnual)
		}
		if s.StartTime.After(monthlyCutoffTime) {
			registerSnapshot(s.StartTime.Format("2006-01"), "monthly", *expireKeepMonthly)
		}
		if s.StartTime.After(weeklyCutoffTime) {
			yyyy, wk := s.StartTime.ISOWeek()
			registerSnapshot(fmt.Sprintf("%04v-%02v", yyyy, wk), "weekly", *expireKeepWeekly)
		}
		if s.StartTime.After(dailyCutoffTime) {
			registerSnapshot(s.StartTime.Format("2006-01-02"), "daily", *expireKeepDaily)
		}
		if s.StartTime.After(hourlyCutoffTime) {
			registerSnapshot(s.StartTime.Format("2006-01-02 15"), "hourly", *expireKeepHourly)
		}

		tm := s.StartTime.Local().Format("2006-01-02 15:04:05 MST")
		if len(keep) > 0 {
			fmt.Printf("  keeping  %v %v\n", tm, keep)
		} else {
			fmt.Printf("  deleting %v\n", tm)
			toDelete = append(toDelete, snapshotNames[i])
		}
	}

	return toDelete
}

func runExpireCommand(context *kingpin.ParseContext) error {
	conn := mustOpenConnection()
	defer conn.Close()

	if *expireKeepLatest+*expireKeepHourly+*expireKeepDaily+*expireKeepWeekly+*expireKeepMonthly+*expireKeepAnnual == 0 {
		return fmt.Errorf("Must pass at least one of --keep-* arguments.")
	}

	fmt.Fprintf(os.Stderr, "Scanning active snapshots...\n")
	snapshotNames, err := conn.Vault.List("B")
	if err != nil {
		return err
	}

	snapshots := loadBackupManifests(conn.Vault, snapshotNames)
	toDelete := expire(snapshots, snapshotNames)

	fmt.Fprintf(os.Stderr, "\n*** ")

	if len(toDelete) == 0 {
		fmt.Fprintf(os.Stderr, "Nothing to delete.\n")
		return nil
	}

	if *expireDelete == "yes" {
		fmt.Fprintf(os.Stderr, "Deleting %v snapshots...\n", len(toDelete))
		for _, n := range toDelete {
			log.Printf("Removing %v", n)
			if err := conn.Vault.Remove(n); err != nil {
				return err
			}
		}
	} else {
		fmt.Fprintf(os.Stderr, "%v snapshot(s) would be deleted. Pass --delete=yes to do it.\n", len(toDelete))
	}

	return nil
}

func init() {
	expireCommand.Action(runExpireCommand)
}
