package cli

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var (
	lsCommand = app.Command("list", "List a directory stored in repository object.").Alias("ls")

	lsCommandLong         = lsCommand.Flag("long", "Long output").Short('l').Bool()
	lsCommandRecursive    = lsCommand.Flag("recursive", "Recursive output").Short('r').Bool()
	lsCommandShowOID      = lsCommand.Flag("show-object-id", "Show object IDs").Short('o').Bool()
	lsCommandErrorSummary = lsCommand.Flag("error-summary", "Emit error summary").Default("true").Bool()
	lsCommandPath         = lsCommand.Arg("object-path", "Path").Required().String()
)

func runLSCommand(ctx context.Context, rep repo.Repository) error {
	dir, err := snapshotfs.FilesystemDirectoryFromIDWithPath(ctx, rep, *lsCommandPath, false)
	if err != nil {
		return errors.Wrap(err, "unable to get filesystem directory entry")
	}

	var prefix string
	if !*lsCommandLong {
		prefix = *lsCommandPath
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
	}

	return listDirectory(ctx, dir, prefix, "")
}

func init() {
	lsCommand.Action(repositoryAction(runLSCommand))
}

func listDirectory(ctx context.Context, d fs.Directory, prefix, indent string) error {
	entries, err := d.Readdir(ctx)
	if err != nil {
		return errors.Wrap(err, "error reading directory")
	}

	for _, e := range entries {
		if err := printDirectoryEntry(ctx, e, prefix, indent); err != nil {
			return errors.Wrap(err, "unable to print directory entry")
		}
	}

	if dws, ok := d.(fs.DirectoryWithSummary); ok && *lsCommandErrorSummary {
		if ds, _ := dws.Summary(ctx); ds != nil && ds.NumFailed > 0 {
			errorColor.Fprintf(os.Stderr, "\nNOTE: Encountered %v errors while snapshotting this directory:\n\n", ds.NumFailed) //nolint:errcheck

			for _, e := range ds.FailedEntries {
				errorColor.Fprintf(os.Stderr, "- Error in \"%v\": %v\n", e.EntryPath, e.Error) //nolint:errcheck
			}
		}
	}

	return nil
}

func printDirectoryEntry(ctx context.Context, e fs.Entry, prefix, indent string) error {
	objectID := e.(object.HasObjectID).ObjectID()
	oid := objectID.String()
	col := defaultColor

	var (
		errorSummary string
		info         string
	)

	if dws, ok := e.(fs.DirectoryWithSummary); ok && *lsCommandErrorSummary {
		if ds, _ := dws.Summary(ctx); ds != nil && ds.NumFailed > 0 {
			errorSummary = fmt.Sprintf(" (%v errors)", ds.NumFailed)
			col = errorColor
		}
	}

	switch {
	case *lsCommandLong:
		info = fmt.Sprintf(
			"%v %12d %v %-34v %v%v",
			e.Mode(),
			e.Size(),
			formatTimestamp(e.ModTime().Local()),
			oid,
			nameToDisplay(prefix, e),
			errorSummary,
		)
	case *lsCommandShowOID:
		info = fmt.Sprintf("%-34v %v%v", oid, nameToDisplay(prefix, e), errorSummary)

	default:
		info = fmt.Sprintf("%v%v", nameToDisplay(prefix, e), errorSummary)
	}

	col.Println(info) //nolint:errcheck

	if *lsCommandRecursive {
		if subdir, ok := e.(fs.Directory); ok {
			if listerr := listDirectory(ctx, subdir, prefix+e.Name()+"/", indent+"  "); listerr != nil {
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
