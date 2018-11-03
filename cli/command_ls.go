package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/kopia/repo"
	"github.com/kopia/repo/object"
)

var (
	lsCommand = app.Command("list", "List a directory stored in repository object.").Alias("ls")

	lsCommandLong      = lsCommand.Flag("long", "Long output").Short('l').Bool()
	lsCommandRecursive = lsCommand.Flag("recursive", "Recursive output").Short('r').Bool()
	lsCommandShowOID   = lsCommand.Flag("show-object-id", "Show object IDs").Short('o').Bool()
	lsCommandPath      = lsCommand.Arg("object-path", "Path").Required().String()
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
	d := snapshotfs.DirectoryEntry(rep, oid, nil)

	entries, err := d.Readdir(ctx)
	if err != nil {
		return err
	}

	maxNameLen := 20
	for _, e := range entries {
		if l := len(nameToDisplay(prefix, e)); l > maxNameLen {
			maxNameLen = l
		}
	}

	maxNameLenString := strconv.Itoa(maxNameLen)

	for _, e := range entries {
		var info string
		objectID := e.(object.HasObjectID).ObjectID()
		oid := objectID.String()
		if *lsCommandLong {
			info = fmt.Sprintf(
				"%v %12d %v %-"+maxNameLenString+"s %v",
				e.Mode(),
				e.Size(),
				formatTimestamp(e.ModTime().Local()),
				nameToDisplay(prefix, e),
				oid,
			)
		} else if *lsCommandShowOID {
			info = fmt.Sprintf(
				"%v %v",
				nameToDisplay(prefix, e),
				oid)
		} else {
			info = nameToDisplay(prefix, e)
		}
		fmt.Println(info)
		if *lsCommandRecursive && e.Mode().IsDir() {
			if listerr := listDirectory(ctx, rep, prefix+e.Name()+"/", objectID, indent+"  "); listerr != nil {
				return listerr
			}
		}
	}

	return nil
}

func nameToDisplay(prefix string, e fs.Entry) string {
	suffix := ""
	if e.IsDir() {
		suffix = "/"

	}
	if *lsCommandLong || *lsCommandRecursive {
		return prefix + e.Name() + suffix
	}

	return e.Name()
}
