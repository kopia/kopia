package cli

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/kopia/kopia/snapshot"

	"github.com/kopia/kopia/repo"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	showCommand = objectCommands.Command("show", "Show contents of a repository object.")

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

func showObject(r *repo.Repository, oid repo.ObjectID) error {
	var rd io.ReadCloser

	rd, err := r.Open(oid)
	if err != nil {
		return err
	}
	defer rd.Close()

	if *showUnzip {
		gz, err := gzip.NewReader(rd)
		if err != nil {
			return fmt.Errorf("unable to open gzip stream: %v", err)
		}

		rd = gz
	}

	var buf1, buf2 bytes.Buffer
	if *showJSON {
		if _, err := io.Copy(&buf1, rd); err != nil {
			return err
		}

		if err := json.Indent(&buf2, buf1.Bytes(), "", "  "); err != nil {
			return err
		}

		rd = ioutil.NopCloser(&buf2)
	}

	if _, err := io.Copy(os.Stdout, rd); err != nil {
		return err
	}

	return nil
}

func init() {
	showCommand.Action(runShowCommand)
}
