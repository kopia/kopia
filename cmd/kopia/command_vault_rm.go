package main

import "gopkg.in/alecthomas/kingpin.v2"

var (
	vaultRemoveCommand = vaultCommands.Command("rm", "Remove vault items")
	vaultRemoveItems   = vaultRemoveCommand.Arg("item", "Items to remove").Strings()
)

func init() {
	vaultRemoveCommand.Action(removeVaultItem)
}

func removeVaultItem(context *kingpin.ParseContext) error {
	vlt, err := openVault()
	if err != nil {
		return err
	}

	for _, v := range *vaultRemoveItems {
		if err := vlt.Remove(v); err != nil {
			return err
		}
	}

	return nil
}
