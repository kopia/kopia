package cli

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/rs/zerolog/log"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

var (
	policySetCommand = policyCommands.Command("set", "Set snapshot policy for a single directory, user@host or a global policy.")
	policySetTargets = policySetCommand.Arg("target", "Target of a policy ('global','user@host','@host') or a path").Strings()
	policySetGlobal  = policySetCommand.Flag("global", "Set global policy").Bool()

	// Frequency
	policySetFrequency = policySetCommand.Flag("min-duration-between-backups", "Minimum duration between snapshots").DurationList()

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

	// General policy.
	policySetInherit = policySetCommand.Flag("inherit", "Enable or disable inheriting policies from the parent").BoolList()
)

func init() {
	policySetCommand.Action(repositoryAction(setPolicy))
}

func setPolicy(ctx context.Context, rep *repo.Repository) error {
	mgr := snapshot.NewPolicyManager(rep)

	targets, err := policyTargets(policySetGlobal, policySetTargets)
	if err != nil {
		return err
	}

	for _, target := range targets {
		p, err := mgr.GetDefinedPolicy(target)
		if err == snapshot.ErrPolicyNotFound {
			p = &snapshot.Policy{}
		}

		if err := setPolicyFromFlags(target, p); err != nil {
			return err
		}

		if err := mgr.SetPolicy(target, p); err != nil {
			return fmt.Errorf("can't save policy for %v: %v", target, err)
		}
	}

	return nil
}

func setPolicyFromFlags(target snapshot.SourceInfo, p *snapshot.Policy) error {
	cases := []struct {
		desc      string
		max       **int
		flagValue *string
	}{
		{"number of annual backups to keep", &p.RetentionPolicy.KeepAnnual, policySetKeepAnnual},
		{"number of monthly backups to keep", &p.RetentionPolicy.KeepMonthly, policySetKeepMonthly},
		{"number of weekly backups to keep", &p.RetentionPolicy.KeepWeekly, policySetKeepWeekly},
		{"number of daily backups to keep", &p.RetentionPolicy.KeepDaily, policySetKeepDaily},
		{"number of hourly backups to keep", &p.RetentionPolicy.KeepHourly, policySetKeepHourly},
		{"number of latest backups to keep", &p.RetentionPolicy.KeepLatest, policySetKeepLatest},
	}

	for _, c := range cases {
		if err := applyPolicyNumber(target, c.desc, c.max, *c.flagValue); err != nil {
			return err
		}
	}

	// It's not really a list, just optional boolean.
	for _, inherit := range *policySetInherit {
		p.NoParent = !inherit
	}

	if *policySetClearExclude {
		p.FilesPolicy.Exclude = nil
	} else {
		p.FilesPolicy.Exclude = addRemoveDedupeAndSort(p.FilesPolicy.Exclude, *policySetAddExclude, *policySetRemoveExclude)
	}
	if *policySetClearInclude {
		p.FilesPolicy.Include = nil
	} else {
		p.FilesPolicy.Include = addRemoveDedupeAndSort(p.FilesPolicy.Include, *policySetAddInclude, *policySetRemoveInclude)
	}

	for _, freq := range *policySetFrequency {
		p.SchedulingPolicy.Frequency = freq
	}

	return nil
}

func addRemoveDedupeAndSort(base, add, remove []string) []string {
	entries := map[string]bool{}
	for _, b := range base {
		entries[b] = true
	}
	for _, b := range add {
		entries[b] = true
	}
	for _, b := range remove {
		delete(entries, b)
	}

	var s []string
	for k := range entries {
		s = append(s, k)
	}
	sort.Strings(s)
	return s
}

func applyPolicyNumber(src snapshot.SourceInfo, desc string, val **int, str string) error {
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
