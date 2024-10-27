package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

type commandList struct {
	long         bool
	recursive    bool
	showOID      bool
	errorSummary bool
	path         string

	out textOutput
}

func (c *commandList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List a directory stored in repository object.").Alias("ls")

	cmd.Flag("long", "Long output").Short('l').BoolVar(&c.long)
	cmd.Flag("recursive", "Recursive output").Short('r').BoolVar(&c.recursive)
	cmd.Flag("show-object-id", "Show object IDs").Short('o').BoolVar(&c.showOID)
	cmd.Flag("error-summary", "Emit error summary").Default("true").BoolVar(&c.errorSummary)
	cmd.Arg("object-path", "Path").Required().StringVar(&c.path)
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.out.setup(svc)
}

func (c *commandList) run(ctx context.Context, rep repo.Repository) error {
	dir, err := snapshotfs.FilesystemDirectoryFromIDWithPath(ctx, rep, c.path, false)
	if err != nil {
		return errors.Wrap(err, "unable to get filesystem directory entry")
	}

	var prefix string
	if !c.long {
		prefix = c.path
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
	}

	return c.listDirectory(ctx, dir, prefix, "")
}

func (c *commandList) listDirectory(ctx context.Context, d fs.Directory, prefix, indent string) error {
	iter, err := d.Iterate(ctx)
	if err != nil {
		return err //nolint:wrapcheck
	}
	defer iter.Close()

	e, err := iter.Next(ctx)
	for e != nil {
		if err2 := c.printDirectoryEntry(ctx, e, prefix, indent); err2 != nil {
			return err2
		}

		e, err = iter.Next(ctx)
	}

	if err != nil {
		return err //nolint:wrapcheck
	}

	if dws, ok := d.(fs.DirectoryWithSummary); ok && c.errorSummary {
		if ds, _ := dws.Summary(ctx); ds != nil && ds.FatalErrorCount > 0 {
			errorColor.Fprintf(c.out.stderr(), "\nNOTE: Encountered %v errors while snapshotting this directory:\n\n", ds.FatalErrorCount) //nolint:errcheck

			for _, e := range ds.FailedEntries {
				errorColor.Fprintf(c.out.stderr(), "- Error in \"%v\": %v\n", e.EntryPath, e.Error) //nolint:errcheck
			}
		}
	}

	return nil
}

func (c *commandList) printDirectoryEntry(ctx context.Context, e fs.Entry, prefix, indent string) error {
	hoid, ok := e.(object.HasObjectID)
	if !ok {
		return errors.New("entry without object ID")
	}

	objectID := hoid.ObjectID()
	oid := objectID.String()
	col := defaultColor

	var (
		errorSummary string
		info         string
	)

	if dws, ok := e.(fs.DirectoryWithSummary); ok && c.errorSummary {
		if ds, _ := dws.Summary(ctx); ds != nil && ds.FatalErrorCount > 0 {
			errorSummary = fmt.Sprintf(" (%v errors)", ds.FatalErrorCount)
			col = errorColor
		}
	}

	switch {
	case c.long:
		info = fmt.Sprintf(
			"%v %12d %v %-34v %v%v",
			e.Mode(),
			e.Size(),
			formatTimestamp(e.ModTime().Local()),
			oid,
			c.nameToDisplay(prefix, e),
			errorSummary,
		)
	case c.showOID:
		info = fmt.Sprintf("%-34v %v%v", oid, c.nameToDisplay(prefix, e), errorSummary)

	default:
		info = fmt.Sprintf("%v%v", c.nameToDisplay(prefix, e), errorSummary)
	}

	col.Fprintln(c.out.stdout(), info) //nolint:errcheck

	if c.recursive {
		if subdir, ok := e.(fs.Directory); ok {
			if listerr := c.listDirectory(ctx, subdir, prefix+e.Name()+"/", indent+"  "); listerr != nil {
				return listerr
			}
		}
	}

	return nil
}

func (c *commandList) nameToDisplay(prefix string, e fs.Entry) string {
	suffix := ""
	if e.IsDir() {
		suffix = "/"
	}

	if c.long || c.recursive {
		return prefix + e.Name() + suffix
	}

	return e.Name()
}
