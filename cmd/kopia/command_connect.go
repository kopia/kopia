package main

import (
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	connectCommand = app.Command("connect", "Connect to a vault.")
)

func init() {
	connectCommand.Action(runConnectCommand)
}

func runConnectCommand(context *kingpin.ParseContext) error {
	vlt, err := openVaultSpecifiedByFlag()
	if err != nil {
		return fmt.Errorf("unable to open vault: %v", err)
	}
	if err := persistVaultConfig(vlt); err != nil {
		return err
	}

	fmt.Println("Connected to vault:", *vaultPath)

	return err
}
