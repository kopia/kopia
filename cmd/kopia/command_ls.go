package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/repofs"
	"github.com/kopia/kopia/repo"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	lsCommand = app.Command("ls", "List a directory stored in repository object.")

	lsCommandLong = lsCommand.Flag("long", "Long output").Short('l').Bool()
	lsCommandPath = lsCommand.Arg("path", "Path").Required().String()
)

func runLSCommand(context *kingpin.ParseContext) error {
	vlt, r := mustOpenVaultAndRepository()
	defer vlt.Close()
	defer r.Close()

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

	d := repofs.Directory(r, oid)

	entries, err := d.Readdir()
	if err != nil {
		return err
	}

	listDirectory(prefix, entries, *lsCommandLong)

	return nil
}

func init() {
	lsCommand.Action(runLSCommand)
}

func listDirectory(prefix string, entries fs.Entries, longFormat bool) {
	maxNameLen := 20
	for _, e := range entries {
		if l := len(e.Metadata().Name); l > maxNameLen {
			maxNameLen = l
		}
	}

	maxNameLenString := strconv.Itoa(maxNameLen)

	for _, e := range entries {
		m := e.Metadata()
		var info string
		if longFormat {
			objectID := e.(repo.HasObjectID).ObjectID()
			var oid string
			if objectID.BinaryContent != nil {
				oid = "<inline binary content>"
			} else if objectID.TextContent != "" {
				oid = "<inline text content>"
			} else {
				oid = objectID.UIString()
			}
			info = fmt.Sprintf(
				"%v %9d %v %-"+maxNameLenString+"s %v",
				m.FileMode(),
				m.FileSize,
				m.ModTime.Local().Format("02 Jan 06 15:04:05"),
				m.Name,
				oid,
			)
		} else {
			var suffix string
			if m.FileMode().IsDir() {
				suffix = "/"
			}

			info = prefix + m.Name + suffix
		}
		fmt.Println(info)
	}
}
