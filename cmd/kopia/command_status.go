package main

import (
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

	f := v.RepositoryFormat()

	fmt.Println("Repository:")
	fmt.Println("  Version:         ", f.Version)
	fmt.Println("  Secret:          ", len(f.Secret), "bytes")
	fmt.Println("  ID Format:       ", f.ObjectFormat)
	fmt.Println("  Blob Size:       ", f.MaxBlobSize)
	fmt.Println("  Inline Blob Size:", f.MaxInlineBlobSize)

	return nil
}
