package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/kopia/kopia/fs/repofs"
	"github.com/kopia/kopia/vault"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	expireCommand = app.Command("expire", "Remove old backups.")

	expirationPolicies = map[string]func(){
		"keep-all": expirationPolicyKeepAll,
		"manual":   expirationPolicyManual,
		"default":  expirationPolicyDefault,
	}

	expireKeepLatest  = expireCommand.Flag("keep-latest", "Number of most recent backups to keep per source").Int()
	expireKeepHourly  = expireCommand.Flag("keep-hourly", "Number of most-recent hourly backups to keep per source").Int()
	expireKeepDaily   = expireCommand.Flag("keep-daily", "Number of most-recent daily backups to keep per source").Int()
	expireKeepWeekly  = expireCommand.Flag("keep-weekly", "Number of most-recent weekly backups to keep per source").Int()
	expireKeepMonthly = expireCommand.Flag("keep-monthly", "Number of most-recent monthly backups to keep per source").Int()
	expireKeepAnnual  = expireCommand.Flag("keep-annual", "Number of most-recent annual backups to keep per source").Int()
	expirePolicy      = expireCommand.Flag("policy", "Expiration policy to use: "+strings.Join(expirationPolicyNames(), ",")).Required().Enum(expirationPolicyNames()...)
	expireHost        = expireCommand.Flag("host", "Expire backups from a given host").Default("").String()
	expireUser        = expireCommand.Flag("user", "Expire backups from a given user").Default("").String()
	expireAll         = expireCommand.Flag("all", "Expire all backups").Bool()
	expirePaths       = expireCommand.Arg("path", "Expire backups for a given paths only").Strings()

	expireDelete = expireCommand.Flag("delete", "Whether to actually delete backups").Default("no").String()
)

func expirationPolicyNames() []string {
	var keys []string
	for k := range expirationPolicies {
		keys = append(keys, k)
	}
	return keys
}

func expirationPolicyKeepAll() {
	*expireKeepLatest = 1000000
	*expireKeepHourly = 1000000
	*expireKeepDaily = 1000000
	*expireKeepWeekly = 1000000
	*expireKeepMonthly = 1000000
	*expireKeepAnnual = 1000000
}

func expirationPolicyDefault() {
	*expireKeepLatest = 1
	*expireKeepHourly = 48
	*expireKeepDaily = 7
	*expireKeepWeekly = 4
	*expireKeepMonthly = 4
	*expireKeepAnnual = 0
}

func expirationPolicyManual() {
}

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
			fmt.Printf("  keeping  %v %v\n", tm, strings.Join(keep, ","))
		} else {
			fmt.Printf("  deleting %v\n", tm)
			toDelete = append(toDelete, snapshotNames[i])
		}
	}

	return toDelete
}

func getSnapshotNamesToExpire(v *vault.Vault) ([]string, error) {
	if !*expireAll && len(*expirePaths) == 0 {
		return nil, fmt.Errorf("Must specify paths to expire or --all")
	}

	if *expireAll {
		fmt.Fprintf(os.Stderr, "Scanning all active snapshots...\n")
		return v.List("B")
	}

	var result []string

	for _, p := range *expirePaths {
		si, err := repofs.ParseSourceSnashotInfo(p, *expireHost, *expireUser)
		if err != nil {
			return nil, fmt.Errorf("unable to parse %v: %v", p, err)
		}

		log.Printf("Looking for backups of %v", si)

		matches, err := v.List("B" + si.HashString())
		if err != nil {
			return nil, fmt.Errorf("error listing backups for %v: %v", si, err)
		}

		log.Printf("Found %v backups of %v", len(matches), si)

		result = append(result, matches...)
	}

	return result, nil
}

func runExpireCommand(context *kingpin.ParseContext) error {
	conn := mustOpenConnection()
	defer conn.Close()

	log.Printf("Applying expiration policy: %v (override with --policy)", *expirePolicy)
	expirationPolicies[*expirePolicy]()

	if *expireKeepLatest+*expireKeepHourly+*expireKeepDaily+*expireKeepWeekly+*expireKeepMonthly+*expireKeepAnnual == 0 {
		return fmt.Errorf("Must pass at least one of --keep-* arguments.")
	}

	log.Printf("Will keep:")
	log.Printf("  %v latest backups", *expireKeepLatest)
	log.Printf("  %v last hourly backups", *expireKeepHourly)
	log.Printf("  %v last daily backups", *expireKeepDaily)
	log.Printf("  %v last weekly backups", *expireKeepWeekly)
	log.Printf("  %v last monthly backups", *expireKeepMonthly)
	log.Printf("  %v last annual backups", *expireKeepAnnual)

	snapshotNames, err := getSnapshotNamesToExpire(conn.Vault)
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
