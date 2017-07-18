package cli

import (
	"fmt"
	"log"
	"strconv"

	"github.com/kopia/kopia/snapshot"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	policySetCommand = policyCommands.Command("set", "Set snapshot policy for a single directory, user@host or a global policy.")
	policySetTargets = policySetCommand.Arg("target", "Target of a policy ('global','user@host','@host') or a path").Strings()
	policySetGlobal  = policySetCommand.Flag("global", "Set global policy").Bool()

	// Frequency
	policySetFrequency = policySetCommand.Flag("min-duration-between-backups", "Minimum duration between snapshots").Duration()

	// Expiration policies.
	policySetKeepLatest  = policySetCommand.Flag("keep-latest", "Number of most recent backups to keep per source (or 'inherit')").String()
	policySetKeepHourly  = policySetCommand.Flag("keep-hourly", "Number of most-recent hourly backups to keep per source (or 'inherit')").String()
	policySetKeepDaily   = policySetCommand.Flag("keep-daily", "Number of most-recent daily backups to keep per source (or 'inherit')").String()
	policySetKeepWeekly  = policySetCommand.Flag("keep-weekly", "Number of most-recent weekly backups to keep per source (or 'inherit')").String()
	policySetKeepMonthly = policySetCommand.Flag("keep-monthly", "Number of most-recent monthly backups to keep per source (or 'inherit')").String()
	policySetKeepAnnual  = policySetCommand.Flag("keep-annual", "Number of most-recent annual backups to keep per source (or 'inherit')").String()

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

	targets, err := policyTargets(policySetGlobal, policySetTargets)
	if err != nil {
		return err
	}

	for _, target := range targets {
		p, err := mgr.GetPolicy(target)
		if err == snapshot.ErrPolicyNotFound {
			p = &snapshot.Policy{
				Source: *target,
			}
		}

		if err := applyPolicyNumber(target, "number of annual backups to keep", &p.Expiration.KeepAnnual, *policySetKeepAnnual); err != nil {
			return err
		}

		if err := applyPolicyNumber(target, "number of monthly backups to keep", &p.Expiration.KeepMonthly, *policySetKeepMonthly); err != nil {
			return err
		}

		if err := applyPolicyNumber(target, "number of weekly backups to keep", &p.Expiration.KeepWeekly, *policySetKeepWeekly); err != nil {
			return err
		}

		if err := applyPolicyNumber(target, "number of daily backups to keep", &p.Expiration.KeepDaily, *policySetKeepDaily); err != nil {
			return err
		}

		if err := applyPolicyNumber(target, "number of hourly backups to keep", &p.Expiration.KeepHourly, *policySetKeepHourly); err != nil {
			return err
		}

		if err := applyPolicyNumber(target, "number of latest backups to keep", &p.Expiration.KeepLatest, *policySetKeepLatest); err != nil {
			return err
		}

		if err := mgr.SavePolicy(p); err != nil {
			return fmt.Errorf("can't save policy for %v: %v", target, err)
		}
	}

	return nil
}

func applyPolicyNumber(src *snapshot.SourceInfo, desc string, val **int, str string) error {
	if str == "" {
		// not changed
		return nil
	}

	if str == "inherit" || str == "default" {
		log.Printf("Resetting %v for %q to a default value inherited from parent.", desc, src)
		*val = nil
		return nil
	}

	v, err := strconv.ParseInt(str, 10, 32)
	if err != nil {
		return fmt.Errorf("can't parse the %v %q: %v", desc, str, err)
	}

	i := int(v)
	log.Printf("Setting %v on %q to %v.", desc, src, i)
	*val = &i
	return nil
}
