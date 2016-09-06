package main

import (
	"encoding/hex"
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	statusCommand       = app.Command("status", "Display status information.")
	statusRevealSecrets = statusCommand.Flag("secrets", "Reveal secrets").Bool()
)

func init() {
	statusCommand.Action(runRepositoryInfoCommand)
}

func runRepositoryInfoCommand(context *kingpin.ParseContext) error {
	conn := mustOpenConnection()

	vf := conn.Vault.Format
	fmt.Println("Vault:")
	fmt.Println("  Version:         ", vf.Version)
	fmt.Println("  Unique ID:       ", hex.EncodeToString(vf.UniqueID))
	fmt.Println("  Encryption:      ", vf.Encryption)
	fmt.Println("  Checksum:        ", vf.Checksum)
	fmt.Println("  Storage:         ", conn.Vault.Storage)
	fmt.Println()

	rc := conn.Vault.RepoConfig
	f := rc.Format

	fmt.Println("Repository:")
	fmt.Println("  Version:         ", f.Version)
	if *statusRevealSecrets {
		fmt.Println("  Secret:          ", hex.EncodeToString(f.Secret))
	} else {
		fmt.Println("  Secret:          ", len(f.Secret), "bytes")
	}
	fmt.Println("  ID Format:       ", f.ObjectFormat)
	fmt.Println("  Blob Size:       ", f.MaxBlockSize/1024, "KB")
	fmt.Println("  Inline Blob Size:", f.MaxInlineContentLength/1024, "KB")
	fmt.Println("  Storage:         ", conn.Repository.Storage)

	return nil
}
