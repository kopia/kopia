package main

import (
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	connectCommand = app.Command("connect", "Create new vault and optionally connect to it")
)

func init() {
	connectCommand.Action(runConnectCommand)
}

func runConnectCommand(context *kingpin.ParseContext) error {
	vlt, err := openVaultSpecifiedByFlag()
	if err != nil {
		return fmt.Errorf("unable to open vault: %v", err)
	}
	persistVaultConfig(vlt)
	fmt.Println("Connected to vault:", *vaultPath)

	return err
}
