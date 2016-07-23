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

	var prefix string
	if !*lsCommandLong {
		prefix = *lsCommandPath
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
	}

	dir := fs.NewRootDirectoryFromRepository(mgr, oid)

	if *lsCommandLong {
		listDirectory(dir)
	} else {
		entries, err := dir.Readdir()
		if err != nil {
			return err
		}
		for _, e := range entries {
			m := e.Metadata()
			var suffix string
			if m.FileMode.IsDir() {
				suffix = "/"
			}

			fmt.Println(prefix + m.Name + suffix)
		}
	}

	return nil
}

func init() {
	lsCommand.Action(runLSCommand)
}
