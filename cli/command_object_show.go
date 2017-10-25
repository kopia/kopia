package cli

import (
	"io"

	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/snapshot"

	"github.com/kopia/kopia/repo"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	showCommand = objectCommands.Command("show", "Show contents of a repository object.").Alias("cat")

	showObjectIDs = showCommand.Arg("id", "IDs of objects to show").Required().Strings()
	showJSON      = showCommand.Flag("json", "Pretty-print JSON content").Short('j').Bool()
	showUnzip     = showCommand.Flag("unzip", "Transparently unzip the content").Short('z').Bool()
)

func runShowCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	mgr := snapshot.NewManager(rep)

	for _, oidString := range *showObjectIDs {
		oid, err := parseObjectID(mgr, oidString)
		if err != nil {
			return err
		}

		if err := showObject(rep, oid); err != nil {
			return err
		}
	}

	return nil
}

func showObject(r *repo.Repository, oid object.ID) error {
	var rd io.ReadCloser

	rd, err := r.Objects.Open(oid)
	if err != nil {
		return err
	}
	defer rd.Close()

	return showContent(rd, *showUnzip, *showJSON)
}

func init() {
	showCommand.Action(runShowCommand)
}
