package main

import (
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	vaultListCommand = vaultCommands.Command("list", "List contents of a vault")
	vaultListPrefix  = vaultListCommand.Flag("prefix", "Prefix").String()
)

func init() {
	vaultListCommand.Action(listVaultContents)
}

func listVaultContents(context *kingpin.ParseContext) error {
	conn := mustOpenConnection()

	entries, err := conn.Vault.List(*vaultListPrefix)
	if err != nil {
		return err
	}

	for _, e := range entries {
		fmt.Println(e)
	}

	return nil
}
