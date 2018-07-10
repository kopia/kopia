package cli

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

var (
	policySetCommand = policyCommands.Command("set", "Set snapshot policy for a single directory, user@host or a global policy.")
	policySetTargets = policySetCommand.Arg("target", "Target of a policy ('global','user@host','@host') or a path").Strings()
	policySetGlobal  = policySetCommand.Flag("global", "Set global policy").Bool()

	// Frequency
	policySetInterval   = policySetCommand.Flag("snapshot-interval", "Interval between snapshots").DurationList()
	policySetTimesOfDay = policySetCommand.Flag("snapshot-time", "Times of day when to take snapshot (HH:mm)").Strings()

	// Expiration policies.
	policySetKeepLatest  = policySetCommand.Flag("keep-latest", "Number of most recent backups to keep per source (or 'inherit')").PlaceHolder("N").String()
	policySetKeepHourly  = policySetCommand.Flag("keep-hourly", "Number of most-recent hourly backups to keep per source (or 'inherit')").PlaceHolder("N").String()
	policySetKeepDaily   = policySetCommand.Flag("keep-daily", "Number of most-recent daily backups to keep per source (or 'inherit')").PlaceHolder("N").String()
	policySetKeepWeekly  = policySetCommand.Flag("keep-weekly", "Number of most-recent weekly backups to keep per source (or 'inherit')").PlaceHolder("N").String()
	policySetKeepMonthly = policySetCommand.Flag("keep-monthly", "Number of most-recent monthly backups to keep per source (or 'inherit')").PlaceHolder("N").String()
	policySetKeepAnnual  = policySetCommand.Flag("keep-annual", "Number of most-recent annual backups to keep per source (or 'inherit')").PlaceHolder("N").String()

	// Files to include (by default everything).
	policySetAddInclude    = policySetCommand.Flag("add-include", "List of paths to add to the include list").PlaceHolder("PATTERN").Strings()
	policySetRemoveInclude = policySetCommand.Flag("remove-include", "List of paths to remove from the include list").PlaceHolder("PATTERN").Strings()
	policySetClearInclude  = policySetCommand.Flag("clear-include", "Clear list of paths in the include list").Bool()

	// Files to exclude.
	policySetAddExclude    = policySetCommand.Flag("add-exclude", "List of paths to add to the exclude list").PlaceHolder("PATTERN").Strings()
	policySetRemoveExclude = policySetCommand.Flag("remove-exclude", "List of paths to remove from the exclude list").PlaceHolder("PATTERN").Strings()
	policySetClearExclude  = policySetCommand.Flag("clear-exclude", "Clear list of paths in the exclude list").Bool()
	policySetMaxFileSize   = policySetCommand.Flag("max-file-size", "Exclude files above given size").PlaceHolder("N").String()

	// General policy.
	policySetInherit = policySetCommand.Flag("inherit", "Enable or disable inheriting policies from the parent").BoolList()
)

func init() {
	policySetCommand.Action(repositoryAction(setPolicy))
}

func setPolicy(ctx context.Context, rep *repo.Repository) error {
	mgr := snapshot.NewPolicyManager(rep)

	targets, err := policyTargets(mgr, policySetGlobal, policySetTargets)
	if err != nil {
		return err
	}

	for _, target := range targets {
		p, err := mgr.GetDefinedPolicy(target)
		if err == snapshot.ErrPolicyNotFound {
			p = &snapshot.Policy{}
		}

		printStderr("Setting policy for %v\n", target)
		changeCount := 0

		if err := setPolicyFromFlags(target, p, &changeCount); err != nil {
			return err
		}

		if changeCount == 0 {
			return fmt.Errorf("no changes specified")
		}

		if err := mgr.SetPolicy(target, p); err != nil {
			return fmt.Errorf("can't save policy for %v: %v", target, err)
		}
	}

	return nil
}

func setPolicyFromFlags(target snapshot.SourceInfo, p *snapshot.Policy, changeCount *int) error {
	if err := setRetentionPolicyFromFlags(&p.RetentionPolicy, changeCount); err != nil {
		return fmt.Errorf("retention policy: %v", err)
	}

	if err := setFilesPolicyFromFlags(&p.FilesPolicy, changeCount); err != nil {
		return fmt.Errorf("files policy: %v", err)
	}

	if err := setSchedulingPolicyFromFlags(&p.SchedulingPolicy, changeCount); err != nil {
		return fmt.Errorf("scheduling policy: %v", err)
	}

	if err := applyPolicyNumber("maximum file size", &p.FilesPolicy.MaxSize, *policySetMaxFileSize, changeCount); err != nil {
		return fmt.Errorf("maximum file size: %v", err)
	}

	// It's not really a list, just optional boolean, last one wins.
	for _, inherit := range *policySetInherit {
		*changeCount++
		p.NoParent = !inherit
	}

	return nil
}

func setFilesPolicyFromFlags(fp *snapshot.FilesPolicy, changeCount *int) error {
	if *policySetClearExclude {
		*changeCount++
		printStderr(" - removing all rules for exclude files\n")
		fp.Exclude = nil
	} else {
		fp.Exclude = addRemoveDedupeAndSort("excluded files", fp.Exclude, *policySetAddExclude, *policySetRemoveExclude, changeCount)
	}
	if *policySetClearInclude {
		*changeCount++
		fp.Include = nil
		printStderr(" - removing all rules for include files\n")
	} else {
		fp.Include = addRemoveDedupeAndSort("included files", fp.Include, *policySetAddInclude, *policySetRemoveInclude, changeCount)
	}
	return nil
}

func setRetentionPolicyFromFlags(rp *snapshot.RetentionPolicy, changeCount *int) error {
	cases := []struct {
		desc      string
		max       **int
		flagValue *string
	}{
		{"number of annual backups to keep", &rp.KeepAnnual, policySetKeepAnnual},
		{"number of monthly backups to keep", &rp.KeepMonthly, policySetKeepMonthly},
		{"number of weekly backups to keep", &rp.KeepWeekly, policySetKeepWeekly},
		{"number of daily backups to keep", &rp.KeepDaily, policySetKeepDaily},
		{"number of hourly backups to keep", &rp.KeepHourly, policySetKeepHourly},
		{"number of latest backups to keep", &rp.KeepLatest, policySetKeepLatest},
	}

	for _, c := range cases {
		if err := applyPolicyNumber(c.desc, c.max, *c.flagValue, changeCount); err != nil {
			return err
		}
	}
	return nil
}

func setSchedulingPolicyFromFlags(sp *snapshot.SchedulingPolicy, changeCount *int) error {
	// It's not really a list, just optional value.
	for _, interval := range *policySetInterval {
		*changeCount++
		sp.Interval = &interval
		printStderr(" - setting snapshot interval to %v\n", sp.Interval)
		break
	}

	if len(*policySetTimesOfDay) > 0 {
		var timesOfDay []snapshot.TimeOfDay

		for _, tods := range *policySetTimesOfDay {
			for _, tod := range strings.Split(tods, ",") {
				if tod == "inherit" {
					timesOfDay = nil
					break
				}

				var timeOfDay snapshot.TimeOfDay
				if err := timeOfDay.Parse(tod); err != nil {
					return fmt.Errorf("unable to parse time of day: %v", err)
				}
				timesOfDay = append(timesOfDay, timeOfDay)
			}
		}
		*changeCount++

		sp.TimesOfDay = snapshot.SortAndDedupeTimesOfDay(timesOfDay)

		if timesOfDay == nil {
			printStderr(" - resetting snapshot times of day to default\n")
		} else {
			printStderr(" - setting snapshot times to %v\n", timesOfDay)
		}
	}

	return nil
}

func addRemoveDedupeAndSort(desc string, base, add, remove []string, changeCount *int) []string {
	entries := map[string]bool{}
	for _, b := range base {
		entries[b] = true
	}
	for _, b := range add {
		*changeCount++
		printStderr(" - adding %v to %v\n", b, desc)
		entries[b] = true
	}
	for _, b := range remove {
		*changeCount++
		printStderr(" - removing %v from %v\n", b, desc)
		delete(entries, b)
	}

	var s []string
	for k := range entries {
		s = append(s, k)
	}
	sort.Strings(s)
	return s
}

func applyPolicyNumber(desc string, val **int, str string, changeCount *int) error {
	if str == "" {
		// not changed
		return nil
	}

	if str == "inherit" || str == "default" {
		*changeCount++
		printStderr(" - resetting %v to a default value inherited from parent.\n", desc)
		*val = nil
		return nil
	}

	v, err := strconv.ParseInt(str, 10, 32)
	if err != nil {
		return fmt.Errorf("can't parse the %v %q: %v", desc, str, err)
	}

	i := int(v)
	*changeCount++
	printStderr(" - setting %v to %v.\n", desc, i)
	*val = &i
	return nil
}
