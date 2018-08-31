package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/repofs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
)

const timeFormat = "2006-01-02 15:04:05"
const timeFormatPrecise = "2006-01-02 15:04:05.000000000"

var (
	lsCommand = app.Command("list", "List a directory stored in repository object.").Alias("ls")

	lsCommandLong      = lsCommand.Flag("long", "Long output").Short('l').Bool()
	lsCommandRecursive = lsCommand.Flag("recursive", "Recursive output").Short('r').Bool()
	lsCommandShowOID   = lsCommand.Flag("show-object-id", "Show object IDs").Short('o').Bool()
	lsCommandPath      = lsCommand.Arg("path", "Path").Required().String()
)

func runLSCommand(ctx context.Context, rep *repo.Repository) error {
	oid, err := parseObjectID(ctx, rep, *lsCommandPath)
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

	return listDirectory(ctx, rep, prefix, oid, "")
}

func init() {
	lsCommand.Action(repositoryAction(runLSCommand))
}

func listDirectory(ctx context.Context, rep *repo.Repository, prefix string, oid object.ID, indent string) error {
	d := repofs.DirectoryEntry(rep, oid, nil)

	entries, err := d.Readdir(ctx)
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
		objectID := e.(object.HasObjectID).ObjectID()
		oid := objectID.String()
		if *lsCommandLong {
			info = fmt.Sprintf(
				"%v %12d %v %-"+maxNameLenString+"s %v",
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
			if listerr := listDirectory(ctx, rep, prefix+m.Name+"/", objectID, indent+"  "); listerr != nil {
				return listerr
			}
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
