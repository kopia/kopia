package cli

import (
	"io"
	"os"

	"github.com/kopia/kopia/snapshot"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	catCommand     = app.Command("cat", "Displays contents of a repository object.")
	catCommandPath = catCommand.Arg("path", "Path").Required().String()
)

func runCatCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close() //nolint: errcheck

	mgr := snapshot.NewManager(rep)

	oid, err := parseObjectID(mgr, *catCommandPath)
	if err != nil {
		return err
	}
	r, err := rep.Objects.Open(oid)
	if err != nil {
		return err
	}
	defer r.Close() //nolint:errcheck
	_, err = io.Copy(os.Stdout, r)
	return err
}

func init() {
	catCommand.Action(runCatCommand)
}
