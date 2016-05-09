package main

import (
	"encoding/hex"
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	statusCommand = app.Command("status", "Display status information.")
)

func init() {
	statusCommand.Action(runRepositoryInfoCommand)
}

func runRepositoryInfoCommand(context *kingpin.ParseContext) error {
	v, err := openVault()
	if err != nil {
		return err
	}

	fmt.Println("Vault:")
	fmt.Println("  Address:   ", v.Storage.Configuration().Config.ToURL())
	fmt.Println("  ID:        ", hex.EncodeToString(v.Format.UniqueID))
	fmt.Println("  Encryption:", v.Format.Encryption)
	fmt.Println("  Checksum:  ", v.Format.Checksum)
	fmt.Println("  Master Key:", hex.EncodeToString(v.MasterKey))

	vc, err := v.Repository()
	if err != nil {
		return err
	}

	fmt.Println("Repository:")
	fmt.Println("  Address:  ", vc.Storage.Config.ToURL())
	fmt.Println("  Version:  ", vc.Repository.Version)
	fmt.Println("  Secret:   ", len(vc.Repository.Secret), "bytes")
	fmt.Println("  ID Format:", vc.Repository.ObjectFormat)

	return nil
}
