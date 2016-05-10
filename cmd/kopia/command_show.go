package main

import (
	"io"
	"os"

	"github.com/kopia/kopia/repo"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	showCommand = app.Command("show", "Show contents of a repository object.")

	showObjectIDs = showCommand.Arg("objectID", "IDs of objects to show").Required().Strings()
)

func runShowCommand(context *kingpin.ParseContext) error {
	mgr, err := mustOpenVault().OpenRepository()
	if err != nil {
		return err
	}

	for _, oid := range *showObjectIDs {
		r, err := mgr.Open(repo.ObjectID(oid))
		if err != nil {
			return err
		}

		io.Copy(os.Stdout, r)
	}

	return nil
}

func init() {
	showCommand.Action(runShowCommand)
}
