package main

import (
	"io"
	"os"

	"github.com/kopia/kopia/cas"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	showCommand = app.Command("show", "Show contents of a CAS object.")

	showObjectIDs = showCommand.Arg("objectID", "Directories to back up").Required().Strings()
)

func runShowCommand(context *kingpin.ParseContext) error {
	mgr, err := mustOpenSession().OpenObjectManager()
	if err != nil {
		return err
	}

	for _, oid := range *showObjectIDs {
		r, err := mgr.Open(cas.ObjectID(oid))
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
