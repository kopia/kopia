package main

import (
	"fmt"
	"strings"

	"github.com/kopia/kopia/fs"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	lsCommand = app.Command("ls", "List a directory stored in repository object.")

	lsCommandLong = lsCommand.Flag("long", "Long output").Short('l').Bool()
	lsCommandPath = lsCommand.Arg("path", "Path").Required().String()
)

func runLSCommand(context *kingpin.ParseContext) error {
	vlt := mustOpenVault()
	mgr, err := vlt.OpenRepository()
	if err != nil {
		return err
	}

	oid, err := parseObjectID(*lsCommandPath, vlt)
	if err != nil {
		return err
	}
	r, err := mgr.Open(oid)
	if err != nil {
		return err
	}

	var prefix string
	if !*lsCommandLong {
		prefix = *lsCommandPath
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
	}

	dir, err := fs.ReadDirectory(r, "")
	if err != nil {
		return fmt.Errorf("unable to read directory contents")
	}

	if *lsCommandLong {
		listDirectory(dir)
	} else {
		for _, e := range dir {
			var suffix string
			if e.FileMode.IsDir() {
				suffix = "/"
			}

			fmt.Println(prefix + e.Name + suffix)
		}
	}

	return nil
}

func init() {
	lsCommand.Action(runLSCommand)
}
