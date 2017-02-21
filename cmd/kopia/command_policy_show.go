package main

import (
	"fmt"

	"github.com/kopia/kopia/snapshot"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	policyShowCommand = policyCommands.Command("show", "Show snapshot policy.")
)

func init() {
	policyShowCommand.Action(showPolicy)
}

func showPolicy(context *kingpin.ParseContext) error {
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
