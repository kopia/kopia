package main

import (
	"encoding/json"
	"io"
	"os"

	"github.com/kopia/kopia/repo"

	"github.com/kopia/kopia/fs"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	showCommand = app.Command("show", "Show contents of a repository object.")

	showObjectIDs = showCommand.Arg("id", "IDs of objects to show").Required().Strings()
	showJSON      = showCommand.Flag("json", "Pretty-print JSON content").Short('j').Bool()
	showDir       = showCommand.Flag("dir", "Pretty-print directory content").Short('d').Bool()
)

func runShowCommand(context *kingpin.ParseContext) error {
	vlt := mustOpenVault()
	mgr, err := vlt.OpenRepository()
	if err != nil {
		return err
	}

	for _, oidString := range *showObjectIDs {
		oid, err := parseObjectID(oidString, vlt)
		if err != nil {
			return err
		}

		if err := showObject(mgr, oid); err != nil {
			return err
		}
	}

	return nil
}

func showObject(mgr repo.Repository, oid repo.ObjectID) error {
	switch {
	case *showJSON:
		r, err := mgr.Open(oid)
		if err != nil {
			return err
		}
		defer r.Close()

		var v map[string]interface{}

		if err := json.NewDecoder(r).Decode(&v); err != nil {
			return err
		}

		m, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return err
		}
		os.Stdout.Write(m)
		return nil

	case *showDir:
		metadata := fs.NewRepositoryDirectory(mgr, oid)

		entries, err := metadata.Readdir()
		if err != nil {
			return err
		}

		listDirectory("", entries, true)
		return nil

	default:
		r, err := mgr.Open(oid)
		if err != nil {
			return err
		}
		defer r.Close()

		_, err = io.Copy(os.Stdout, r)
		return err
	}
}

func init() {
	showCommand.Action(runShowCommand)
}
