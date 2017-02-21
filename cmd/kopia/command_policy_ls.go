package main

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
	conn := mustOpenConnection()
	mgr := snapshot.NewManager(conn)

	entries, err := mgr.ListPolicies()
	if err != nil {
		return err
	}

	for _, e := range entries {
		fmt.Println(e)
	}

	return nil
}
