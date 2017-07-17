package main

import (
	"log"

	"github.com/kopia/kopia/snapshot"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	policyRemoveCommand = policyCommands.Command("remove", "Remove snapshot policy for a single directory, user@host or a global policy.").Alias("rm").Alias("delete")
	policyRemoveTargets = policyRemoveCommand.Arg("target", "Target of a policy ('global','user@host','@host') or a path").Strings()
	policyRemoveGlobal  = policyRemoveCommand.Flag("global", "Set global policy").Bool()
)

func init() {
	policyRemoveCommand.Action(removePolicy)
}

func removePolicy(context *kingpin.ParseContext) error {
	conn := mustOpenConnection()
	mgr := snapshot.NewManager(conn.Vault)

	targets, err := policyTargets(policyRemoveGlobal, policyRemoveTargets)
	if err != nil {
		return err
	}

	for _, target := range targets {
		log.Printf("Removing policy on %q...", target)
		if err := mgr.RemovePolicy(target); err != nil {
			return err
		}
	}

	return nil
}
