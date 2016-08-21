package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/kopia/kopia/repo"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	showCommand = app.Command("show", "Show contents of a repository object.")

	showObjectIDs = showCommand.Arg("id", "IDs of objects to show").Required().Strings()
	showJSON      = showCommand.Flag("json", "Pretty-print JSON content").Short('j').Bool()
	showRaw       = showCommand.Flag("raw", "Show raw content (disables format auto-detection)").Short('r').Bool()
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
	r, err := mgr.Open(oid)
	if err != nil {
		return err
	}
	defer r.Close()

	rawdata, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	format := "raw"

	if rawdata[0] == '{' && rawdata[len(rawdata)-1] == '}' {
		format = "json"
	}

	if *showJSON {
		format = "json"
	}

	if *showRaw {
		format = "raw"
	}

	switch format {
	case "json":
		var buf bytes.Buffer

		json.Indent(&buf, rawdata, "", "  ")
		os.Stdout.Write(buf.Bytes())

	default:
		os.Stdout.Write(rawdata)
	}
	return nil
}

func init() {
	showCommand.Action(runShowCommand)
}
