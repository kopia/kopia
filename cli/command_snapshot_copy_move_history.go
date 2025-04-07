package cli

import (
	"context"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

type commandSnapshotCopyMoveHistory struct {
	snapshotCopyOrMoveDryRun      bool
	snapshotCopyOrMoveSource      string
	snapshotCopyOrMoveDestination string
}

func (c *commandSnapshotCopyMoveHistory) setup(svc appServices, parent commandParent, isMove bool) {
	var cmd *kingpin.CmdClause
	if isMove {
		cmd = parent.Command("move-history", snapshotCopyMoveHelp("move"))
	} else {
		cmd = parent.Command("copy-history", snapshotCopyMoveHelp("copy"))
	}

	cmd.Flag("dry-run", "Do not actually copy snapshots, only print what would happen").Short('n').BoolVar(&c.snapshotCopyOrMoveDryRun)
	cmd.Arg("source", "Source (user@host or user@host:path)").Required().StringVar(&c.snapshotCopyOrMoveSource)
	cmd.Arg("destination", "Destination (defaults to current user@host)").StringVar(&c.snapshotCopyOrMoveDestination)

	cmd.Action(svc.repositoryWriterAction(func(ctx context.Context, rep repo.RepositoryWriter) error {
		return c.run(ctx, rep, isMove)
	}))
}

func snapshotCopyMoveHelp(verb string) string {
	return strings.ReplaceAll(`Performs a VERB of the history of snapshots from another user or host.
	This command will VERB snapshot manifests of the specified source to the respective destination.
	This is typically used when renaming a host, switching username or moving directory
	around to maintain snapshot history.

	Both source and destination can be specified using user@host, @host or user@host:/path
	where destination values override the corresponding parts of the source, so both targeted
	and mass VERB is supported.

	Source:             Destination         Behavior
	---------------------------------------------------
	@host1              @host2              VERB snapshots from all users of host1
	@host1              user2@host2         (disallowed as it would potentially collapse users)
	@host1              user2@host2:/path2  (disallowed as it would potentially collapse paths)
	user1@host1         @host2              VERB all snapshots to user1@host2
	user1@host1         user2@host2         VERB all snapshots to user2@host2
	user1@host1         user2@host2:/path2  (disallowed as it would potentially collapse paths)
	user1@host1:/path1  @host2              VERB to user1@host2:/path1
	user1@host1:/path1  user2@host2         VERB to user2@host2:/path1
	user1@host1:/path1  user2@host2:/path2  VERB snapshots from single path.
`, "VERB", verb)
}

// run copies snapshot manifests of the specified source
// to the respective destination. This is typically used when renaming a host,
// switching username or moving directory around to maintain snapshot history.
//
// Both source and destination can be specified using user@host, @host or user@host:/path
// where destination values override the corresponding parts of the source, so both targeted
// and mass copying is supported.
//
// Source:             Destination         Behavior
// ---------------------------------------------------
// @host1              @host2              copy snapshots from all users of host1
// @host1              user2@host2         (disallowed as it would potentially collapse users)
// @host1              user2@host2:/path2  (disallowed as it would potentially collapse paths)
//
// user1@host1         @host2              copy all snapshots to user1@host2
// user1@host1         user2@host2         copy all snapshots to user2@host2
// user1@host1         user2@host2:/path2  (disallowed as it would potentially collapse paths)
//
// user1@host1:/path1  @host2              copy to user1@host2:/path1
// user1@host1:/path1  user2@host2         copy to user2@host2:/path1
// user1@host1:/path1  user2@host2:/path2  copy snapshots from single path.
func (c *commandSnapshotCopyMoveHistory) run(ctx context.Context, rep repo.RepositoryWriter, isMoveCommand bool) error {
	si, di, err := c.getCopySourceAndDestination(rep)
	if err != nil {
		return err
	}

	// At this point si and di are possibly incomplete snapshot.SourceInfo
	// could be hostname, user@hostname or user@hostname:/path

	srcSnapshots, err := snapshot.ListSnapshots(ctx, rep, si)
	if err != nil {
		return errors.Wrap(err, "error listing source snapshots")
	}

	dstSnapshots, err := snapshot.ListSnapshots(ctx, rep, di)
	if err != nil {
		return errors.Wrap(err, "error listing destination snapshots")
	}

	for _, manifest := range srcSnapshots {
		dstSource := getCopyDestination(manifest.Source, di)

		if dstSource == manifest.Source {
			log(ctx).Debugf("%v is the same as destination, ignoring", dstSource)
			continue
		}

		if snapshotExists(dstSnapshots, dstSource, manifest) {
			if isMoveCommand && !c.snapshotCopyOrMoveDryRun {
				log(ctx).Infof("%v (%v) already exists - deleting source", dstSource, formatTimestamp(manifest.StartTime.ToTime()))

				if err := rep.DeleteManifest(ctx, manifest.ID); err != nil {
					return errors.Wrap(err, "unable to delete source manifest")
				}
			} else {
				log(ctx).Infof("%v (%v) already exists", dstSource, formatTimestamp(manifest.StartTime.ToTime()))
			}

			continue
		}

		srcID := manifest.ID

		log(ctx).Infof("%v %v (%v) => %v", c.getCopySnapshotAction(isMoveCommand), manifest.Source, formatTimestamp(manifest.StartTime.ToTime()), dstSource)

		if c.snapshotCopyOrMoveDryRun {
			continue
		}

		manifest.ID = ""
		manifest.Source = dstSource

		if _, err := snapshot.SaveSnapshot(ctx, rep, manifest); err != nil {
			return errors.Wrap(err, "unable to save snapshot")
		}

		if isMoveCommand {
			if err := rep.DeleteManifest(ctx, srcID); err != nil {
				return errors.Wrap(err, "unable to delete source manifest")
			}
		}
	}

	return nil
}

func (c *commandSnapshotCopyMoveHistory) getCopySnapshotAction(isMoveCommand bool) string {
	action := "copying"
	if isMoveCommand {
		action = "moving"
	}

	if c.snapshotCopyOrMoveDryRun {
		action += " (dry run)"
	}

	return action
}

func (c *commandSnapshotCopyMoveHistory) getCopySourceAndDestination(rep repo.RepositoryWriter) (si, di snapshot.SourceInfo, err error) {
	si, err = snapshot.ParseSourceInfo(c.snapshotCopyOrMoveSource, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
	if err != nil {
		return si, di, errors.Wrap(err, "invalid source")
	}

	if c.snapshotCopyOrMoveDestination == "" {
		// no destination - assume current user@hostname
		di.UserName = rep.ClientOptions().Username
		di.Host = rep.ClientOptions().Hostname
	} else {
		di, err = snapshot.ParseSourceInfo(c.snapshotCopyOrMoveDestination, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
		if err != nil {
			return si, di, errors.Wrap(err, "invalid destination")
		}
	}

	if di.Path != "" && si.Path == "" {
		// it is illegal to specify source without path, but destination with a path
		// as it would result in multiple individual paths being squished together.
		return si, di, errors.New("path specified on destination but not source")
	}

	if di.UserName != "" && si.UserName == "" {
		// it is illegal to specify source without username, but destination with a username
		// as it would result in multiple individual paths being squished together.
		return si, di, errors.New("username specified on destination but not source")
	}

	return si, di, nil
}

func snapshotExists(snaps []*snapshot.Manifest, src snapshot.SourceInfo, srcManifest *snapshot.Manifest) bool {
	for _, s := range snaps {
		if src != s.Source {
			continue
		}

		if sameSnapshot(srcManifest, s) {
			return true
		}
	}

	return false
}

// sameSnapshot returns true if snapshot manifests have the same start time and root object ID.
func sameSnapshot(a, b *snapshot.Manifest) bool {
	if !a.StartTime.Equal(b.StartTime) {
		return false
	}

	if a.RootObjectID() != b.RootObjectID() {
		return false
	}

	return true
}

// getCopyDestination returns the source modified by applying non-empty fields specified in the overrides.
func getCopyDestination(source, overrides snapshot.SourceInfo) snapshot.SourceInfo {
	dst := source

	if overrides.Host != "" {
		dst.Host = overrides.Host
	}

	if overrides.UserName != "" {
		dst.UserName = overrides.UserName
	}

	if overrides.Path != "" {
		dst.Path = overrides.Path
	}

	return dst
}
