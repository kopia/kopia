package main

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	vaultShowCommand = vaultCommands.Command("show", "Show contents of a vault item")
	vaultShowID      = vaultShowCommand.Arg("id", "ID of the vault item to show").String()
)

func init() {
	vaultShowCommand.Action(showVaultObject)
}

func showVaultObject(context *kingpin.ParseContext) error {
	v, err := openVault()
	if err != nil {
		return err
	}

	b, err := v.Get(*vaultShowID)
	if err != nil {
		return err
	}

	os.Stdout.Write(b)

	return nil
}
