package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const timeFormat = "02 Jan 06 15:04:05"

var (
	lsCommand = app.Command("list", "List a directory stored in repository object.").Alias("ls")

	lsCommandLong      = lsCommand.Flag("long", "Long output").Short('l').Bool()
	lsCommandRecursive = lsCommand.Flag("recursive", "Recursive output").Short('r').Bool()
	lsCommandShowOID   = lsCommand.Flag("show-object-id", "Show object IDs").Short('o').Bool()
	lsCommandPath      = lsCommand.Arg("path", "Path").Required().String()
)

func runLSCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	mgr := snapshot.NewManager(rep)

	oid, err := parseObjectID(mgr, *lsCommandPath)
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

	return listDirectory(mgr, prefix, oid, "")
}

func init() {
	lsCommand.Action(runLSCommand)
}

func listDirectory(mgr *snapshot.Manager, prefix string, oid repo.ObjectID, indent string) error {
	d := mgr.DirectoryEntry(oid)

	entries, err := d.Readdir()
	if err != nil {
		return err
	}

	maxNameLen := 20
	for _, e := range entries {
		if l := len(nameToDisplay(prefix, e.Metadata())); l > maxNameLen {
			maxNameLen = l
		}
	}

	maxNameLenString := strconv.Itoa(maxNameLen)

	for _, e := range entries {
		m := e.Metadata()
		var info string
		objectID := e.(repo.HasObjectID).ObjectID()
		var oid string
		if objectID.BinaryContent != nil {
			oid = "<inline binary content>"
		} else if objectID.TextContent != "" {
			oid = "<inline text content>"
		} else {
			oid = objectID.String()
		}
		if *lsCommandLong {
			info = fmt.Sprintf(
				"%v %9d %v %-"+maxNameLenString+"s %v",
				m.FileMode(),
				m.FileSize,
				m.ModTime.Local().Format(timeFormat),
				nameToDisplay(prefix, m),
				oid,
			)
		} else if *lsCommandShowOID {
			info = fmt.Sprintf(
				"%v %v",
				nameToDisplay(prefix, m),
				oid)
		} else {
			info = nameToDisplay(prefix, m)
		}
		fmt.Println(info)
		if *lsCommandRecursive && m.FileMode().IsDir() {
			listDirectory(mgr, prefix+m.Name+"/", objectID, indent+"  ")
		}
	}

	return nil
}

func nameToDisplay(prefix string, md *fs.EntryMetadata) string {
	suffix := ""
	if md.FileMode().IsDir() {
		suffix = "/"

	}
	if *lsCommandLong || *lsCommandRecursive {
		return prefix + md.Name + suffix
	}

	return md.Name
}
