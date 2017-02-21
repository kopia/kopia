package main

import (
	"log"

	"github.com/kopia/kopia/snapshot"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	policySetCommand = policyCommands.Command("set", "Set snapshot policy for a single directory, user@host or a global policy.")
	policySetTarget  = policySetCommand.Flag("target", "Target of a policy ('global','user@host','@host') or a path").Required().String()

	// Frequency
	policySetFrequency = policySetCommand.Flag("min-duration-between-backups", "Minimum duration between snapshots").Duration()

	// Expiration policies.
	policySetKeepLatest  = policySetCommand.Flag("keep-latest", "Number of most recent backups to keep per source").Int()
	policySetKeepHourly  = policySetCommand.Flag("keep-hourly", "Number of most-recent hourly backups to keep per source").Int()
	policySetKeepDaily   = policySetCommand.Flag("keep-daily", "Number of most-recent daily backups to keep per source").Int()
	policySetKeepWeekly  = policySetCommand.Flag("keep-weekly", "Number of most-recent weekly backups to keep per source").Int()
	policySetKeepMonthly = policySetCommand.Flag("keep-monthly", "Number of most-recent monthly backups to keep per source").Int()
	policySetKeepAnnual  = policySetCommand.Flag("keep-annual", "Number of most-recent annual backups to keep per source").Int()

	// Files to ignore.
	policySetAddIgnore     = policySetCommand.Flag("add-ignore", "List of paths to add to ignore list").Strings()
	policySetRemoveIgnore  = policySetCommand.Flag("remove-ignore", "List of paths to remove from ignore list").Strings()
	policySetReplaceIgnore = policySetCommand.Flag("set-ignore", "List of paths to replace ignore list with").Strings()
)

func init() {
	policySetCommand.Action(setPolicy)
}

func setPolicy(context *kingpin.ParseContext) error {
	conn := mustOpenConnection()
	mgr := snapshot.NewManager(conn)
	_ = mgr

	target, err := snapshot.ParseSourceInfo(*policySetTarget, getHostName(), getUserName())
	if err != nil {
		return err
	}

	log.Printf("target: %v", target)

	return nil
}
