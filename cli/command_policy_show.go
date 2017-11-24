package cli

import (
	"fmt"
	"os"

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
			fmt.Println(p)
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
