package main

import (
	"io"
	"os"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	catCommand     = app.Command("cat", "Displays contents of a repository object.")
	catCommandPath = catCommand.Arg("path", "Path").Required().String()
)

func runCatCommand(context *kingpin.ParseContext) error {
	vlt := mustOpenVault()
	mgr, err := vlt.OpenRepository()
	if err != nil {
		return err
	}

	oid, err := parseObjectID(*catCommandPath, vlt)
	if err != nil {
		return err
	}
	r, err := mgr.Open(oid)
	if err != nil {
		return err
	}
	io.Copy(os.Stdout, r)
	return nil
}

func init() {
	catCommand.Action(runCatCommand)
}
