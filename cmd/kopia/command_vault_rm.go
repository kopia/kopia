package main

import "gopkg.in/alecthomas/kingpin.v2"

var (
	vaultRemoveCommand = vaultCommands.Command("rm", "Remove vault items").Hidden()
	vaultRemoveItems   = vaultRemoveCommand.Arg("item", "Items to remove").Strings()
)

func init() {
	vaultRemoveCommand.Action(removeVaultItem)
}

func removeVaultItem(context *kingpin.ParseContext) error {
	conn := mustOpenConnection()

	for _, v := range *vaultRemoveItems {
		if err := conn.Vault.Remove(v); err != nil {
			return err
		}
	}

	return nil
}
