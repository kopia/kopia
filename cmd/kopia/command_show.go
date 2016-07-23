package main

import (
	"encoding/json"
	"fmt"
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
		r, err := mgr.Open(oid)
		if err != nil {
			return err
		}

		switch {
		case *showJSON:
			var v map[string]interface{}

			if err := json.NewDecoder(r).Decode(&v); err != nil {
				return err
			}

			m, err := json.MarshalIndent(v, "", "  ")
			if err != nil {
				return err
			}
			os.Stdout.Write(m)
		case *showDir:
			metadata, err := fs.ReadDirectoryMetadataEntries(r, "")
			if err != nil {
				return err
			}

			for _, m := range metadata {
				var oid string
				if m.ObjectID.Type().IsStored() {
					oid = string(m.ObjectID)
				} else if m.ObjectID.Type() == repo.ObjectIDTypeBinary {
					oid = "<inline binary>"
				} else if m.ObjectID.Type() == repo.ObjectIDTypeText {
					oid = "<inline text>"
				}
				info := fmt.Sprintf("%v %9d %v %-30s %v", m.FileMode, m.FileSize, m.ModTime.Local().Format("02 Jan 06 15:04:05"), m.Name, oid)
				fmt.Println(info)
			}

		default:
			io.Copy(os.Stdout, r)
		}
	}

	return nil
}

func init() {
	showCommand.Action(runShowCommand)
}
