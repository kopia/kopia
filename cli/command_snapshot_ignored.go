package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

// commandSnapshotIgnored lists the files and directories that were excluded from snapshots by
// ignore rules (policy ignore rules / .kopiaignore). Recording is only populated when the
// policy's FilesPolicy.IgnoreRecord is enabled; otherwise the list is empty.
type commandSnapshotIgnored struct {
	refs []string

	out textOutput
}

func (c *commandSnapshotIgnored) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("ignored", "List files and directories excluded from snapshots by ignore rules.")
	cmd.Arg("ref", "Snapshot ID (as shown by `kopia snapshot list`) or source path").Required().StringsVar(&c.refs)
	cmd.Action(svc.repositoryReaderAction(c.run))
	c.out.setup(svc)
}

func (c *commandSnapshotIgnored) run(ctx context.Context, rep repo.Repository) error {
	for _, ref := range c.refs {
		// If the ref is an object ID, resolve the snapshot(s) whose root object ID matches —
		// this is the snapshot ID that `kopia snapshot list` shows by default.
		if rootOID, err := object.ParseID(ref); err == nil {
			mans, err := snapshot.FindSnapshotsByRootObjectID(ctx, rep, rootOID)
			if err != nil {
				return errors.Wrapf(err, "error finding snapshots by root ID %v", ref)
			}

			if len(mans) == 0 {
				return errors.Errorf("no snapshot found for ID %q", ref)
			}

			for _, m := range mans {
				c.printSnapshot(m)
			}

			continue
		}

		// Not an ID: treat it as a source reference and show all of its snapshots.
		si, err := snapshot.ParseSourceInfo(ref, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
		if err != nil {
			return errors.Wrapf(err, "unable to parse %q as a snapshot ID or source", ref)
		}

		ids, err := snapshot.ListSnapshotManifests(ctx, rep, &si, nil)
		if err != nil {
			return errors.Wrapf(err, "error listing snapshots for %v", si)
		}

		mans, err := snapshot.LoadSnapshots(ctx, rep, ids)
		if err != nil {
			return errors.Wrapf(err, "error loading snapshots for %v", si)
		}

		for _, m := range mans {
			c.printSnapshot(m)
		}
	}

	return nil
}

func (c *commandSnapshotIgnored) printSnapshot(m *snapshot.Manifest) {
	if len(m.IgnoredEntries) == 0 {
		c.out.printStdout("// snapshot %v (%v): no ignored entries recorded\n", m.ID, m.Source)
		return
	}

	suffix := "ies"
	if len(m.IgnoredEntries) == 1 {
		suffix = "y"
	}

	c.out.printStdout("// snapshot %v (%v): %d ignored entr%s\n", m.ID, m.Source, len(m.IgnoredEntries), suffix)

	for _, e := range m.IgnoredEntries {
		if e.IsDir {
			c.out.printStdout("  %s/    (directory; entire subtree excluded)\n", e.EntryPath)
		} else {
			c.out.printStdout("  %s\n", e.EntryPath)
		}
	}
}
