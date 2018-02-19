package cli

import (
	"bytes"
	"fmt"
	"os"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/policy"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	policyShowCommand   = policyCommands.Command("show", "Show snapshot policy.").Alias("get")
	policyShowEffective = policyShowCommand.Flag("effective", "Show effective policy").Bool()
	policyShowGlobal    = policyShowCommand.Flag("global", "Get global policy").Bool()
	policyShowTargets   = policyShowCommand.Arg("target", "Target to show the policy for").Strings()
)

func init() {
	policyShowCommand.Action(showPolicy)
}

func showPolicy(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	pmgr := policy.NewManager(rep)

	targets, err := policyTargets(policyShowGlobal, policyShowTargets)
	if err != nil {
		return err
	}

	for _, target := range targets {
		var p *policy.Policy
		var policyKind string
		var err error

		if *policyShowEffective {
			p, err = pmgr.GetEffectivePolicy(target.UserName, target.Host, target.Path)
			policyKind = "effective"
		} else {
			p, err = pmgr.GetDefinedPolicy(target.UserName, target.Host, target.Path)
			policyKind = "defined"
		}

		if err == nil {
			fmt.Printf("The %v policy for %q:\n", policyKind, target)
			fmt.Println(policyToString(p))
			continue
		}

		if err == policy.ErrPolicyNotFound {
			fmt.Fprintf(os.Stderr, "No %v policy for %q, pass --effective to compute effective policy used for backups.\n", policyKind, target)
			continue
		}

		return fmt.Errorf("can't get %v policy for %q: %v", policyKind, target, err)
	}

	return nil
}

func policyToString(p *policy.Policy) string {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "Retention policy:\n")
	fmt.Fprintf(&buf, "  keep annual:%v monthly:%v weekly:%v daily:%v hourly:%v latest:%v\n",
		valueOrNotSet(p.RetentionPolicy.KeepAnnual),
		valueOrNotSet(p.RetentionPolicy.KeepMonthly),
		valueOrNotSet(p.RetentionPolicy.KeepWeekly),
		valueOrNotSet(p.RetentionPolicy.KeepDaily),
		valueOrNotSet(p.RetentionPolicy.KeepHourly),
		valueOrNotSet(p.RetentionPolicy.KeepLatest),
	)

	fmt.Fprintf(&buf, "Files policy:\n")

	if len(p.FilesPolicy.Include) == 0 {
		fmt.Fprintf(&buf, "  Include all files\n")
	} else {
		fmt.Fprintf(&buf, "  Include only:\n")
	}
	for _, inc := range p.FilesPolicy.Include {
		fmt.Fprintf(&buf, "    %v\n", inc)
	}
	if len(p.FilesPolicy.Exclude) > 0 {
		fmt.Fprintf(&buf, "  Exclude:\n")
	}
	for _, exc := range p.FilesPolicy.Exclude {
		fmt.Fprintf(&buf, "    %v\n", exc)
	}
	if s := p.FilesPolicy.MaxSize; s != nil {
		fmt.Fprintf(&buf, "  Exclude files above size: %v\n", units.BytesStringBase2(int64(*s)))
	}
	return buf.String()
}

func valueOrNotSet(p *int) string {
	if p == nil {
		return "(none)"
	}

	return fmt.Sprintf("%v", *p)
}
