package cli

import (
	"fmt"

	"github.com/kopia/kopia/snapshot"
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
	conn := mustOpenConnection()
	mgr := snapshot.NewManager(conn.Vault)

	targets, err := policyTargets(policyShowGlobal, policyShowTargets)
	if err != nil {
		return err
	}

	for _, target := range targets {
		var p *snapshot.Policy
		var policyKind string
		var err error

		if *policyShowEffective {
			p, err = mgr.GetEffectivePolicy(target)
			policyKind = "effective"
		} else {
			p, err = mgr.GetPolicy(target)
			policyKind = "defined"
		}

		if err == nil {
			fmt.Printf("The %v policy for %q:\n", policyKind, target)
			fmt.Println(p)
			continue
		}

		if err == snapshot.ErrPolicyNotFound {
			fmt.Printf("No %v policy for %q, pass --effective to compute effective policy used for backups.\n", policyKind, target)
			continue
		}

		return fmt.Errorf("can't get %v policy for %q: %v", policyKind, target, err)
	}

	return nil
}
