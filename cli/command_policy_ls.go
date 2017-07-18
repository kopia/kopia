package cli

import (
	"fmt"

	"github.com/kopia/kopia/snapshot"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	policyListCommand = policyCommands.Command("list", "List policies.").Alias("ls")
)

func init() {
	policyListCommand.Action(listPolicies)
}

func listPolicies(context *kingpin.ParseContext) error {
	rep := mustConnectToRepository(nil)
	mgr := snapshot.NewManager(rep)

	policies, err := mgr.ListPolicies()
	if err != nil {
		return err
	}

	for _, pol := range policies {
		fmt.Println(pol.Source)
	}

	return nil
}
