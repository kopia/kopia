package cli

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
	conn := mustOpenConnection()
	defer conn.Close()

	oid, err := parseObjectID(*catCommandPath, conn.Vault, conn.Repository)
	if err != nil {
		return err
	}
	r, err := conn.Repository.Open(oid)
	if err != nil {
		return err
	}
	io.Copy(os.Stdout, r)
	return nil
}

func init() {
	catCommand.Action(runCatCommand)
}
