package cli

import (
	"fmt"
	"log"
	"sort"
	"strconv"

	"github.com/kopia/kopia/policy"
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
	policySetCommand.Action(setPolicy)
}

func setPolicy(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	mgr := policy.NewManager(rep)

	targets, err := policyTargets(policySetGlobal, policySetTargets)
	if err != nil {
		return err
	}

	for _, target := range targets {
		p, err := mgr.GetDefinedPolicy(target.UserName, target.Host, target.Path)
		if err == policy.ErrPolicyNotFound {
			p = &policy.Policy{}
		}

		if err := applyPolicyNumber(target, "number of annual backups to keep", &p.ExpirationPolicy.KeepAnnual, *policySetKeepAnnual); err != nil {
			return err
		}

		if err := applyPolicyNumber(target, "number of monthly backups to keep", &p.ExpirationPolicy.KeepMonthly, *policySetKeepMonthly); err != nil {
			return err
		}

		if err := applyPolicyNumber(target, "number of weekly backups to keep", &p.ExpirationPolicy.KeepWeekly, *policySetKeepWeekly); err != nil {
			return err
		}

		if err := applyPolicyNumber(target, "number of daily backups to keep", &p.ExpirationPolicy.KeepDaily, *policySetKeepDaily); err != nil {
			return err
		}

		if err := applyPolicyNumber(target, "number of hourly backups to keep", &p.ExpirationPolicy.KeepHourly, *policySetKeepHourly); err != nil {
			return err
		}

		if err := applyPolicyNumber(target, "number of latest backups to keep", &p.ExpirationPolicy.KeepLatest, *policySetKeepLatest); err != nil {
			return err
		}

		// It's not really a list, just optional boolean.
		for _, inherit := range *policySetInherit {
			p.NoParent = !inherit
		}

		for _, path := range *policySetAddExclude {
			p.FilesPolicy.Exclude = addString(p.FilesPolicy.Exclude, path)
		}

		for _, path := range *policySetRemoveExclude {
			p.FilesPolicy.Exclude = removeString(p.FilesPolicy.Exclude, path)
		}

		if *policySetClearExclude {
			p.FilesPolicy.Exclude = nil
		}

		if err := mgr.SetPolicy(target.UserName, target.Host, target.Path, p); err != nil {
			return fmt.Errorf("can't save policy for %v: %v", target, err)
		}
	}

	return nil
}

func addString(p []string, s string) []string {
	p = append(removeString(p, s), s)
	sort.Strings(p)
	return p
}

func removeString(p []string, s string) []string {
	var result []string

	for _, item := range p {
		if item == s {
			continue
		}
		result = append(result, item)
	}
	return result
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
