package cli

import (
	"context"
	"strings"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

var (
	snapshotCopyCommand = snapshotCommands.Command("copy-history", snapshotCopyMoveHelp("copy"))
	snapshotMoveCommand = snapshotCommands.Command("move-history", snapshotCopyMoveHelp("move"))

	snapshotCopyOrMoveDryRun      bool
	snapshotCopyOrMoveSource      string
	snapshotCopyOrMoveDestination string
)

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

func registerSnapshotCopyFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("dry-run", "Do not actually copy snapshots, only print what would happen").Short('n').BoolVar(&snapshotCopyOrMoveDryRun)
	cmd.Arg("source", "Source (user@host or user@host:path)").Required().StringVar(&snapshotCopyOrMoveSource)
	cmd.Arg("destination", "Destination (defaults to current user@host)").StringVar(&snapshotCopyOrMoveDestination)
}

// runSnapshotCopyCommand copies snapshot manifests of the specified source
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
func runSnapshotCopyCommand(ctx context.Context, rep repo.Repository, isMoveCommand bool) error {
	si, di, err := getCopySourceAndDestination(rep)
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
			if isMoveCommand && !snapshotCopyOrMoveDryRun {
				log(ctx).Infof("%v (%v) already exists - deleting source", dstSource, formatTimestamp(manifest.StartTime))

				if err := rep.DeleteManifest(ctx, manifest.ID); err != nil {
					return errors.Wrap(err, "unable to delete source manifest")
				}
			} else {
				log(ctx).Infof("%v (%v) already exists", dstSource, formatTimestamp(manifest.StartTime))
			}

			continue
		}

		srcID := manifest.ID

		log(ctx).Infof("%v %v (%v) => %v", getCopySnapshotAction(isMoveCommand), manifest.Source, formatTimestamp(manifest.StartTime), dstSource)

		if snapshotCopyOrMoveDryRun {
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

func getCopySnapshotAction(isMoveCommand bool) string {
	action := "copying"
	if isMoveCommand {
		action = "moving"
	}

	if snapshotCopyOrMoveDryRun {
		action += " (dry run)"
	}

	return action
}

func getCopySourceAndDestination(rep repo.Repository) (si, di snapshot.SourceInfo, err error) {
	si, err = snapshot.ParseSourceInfo(snapshotCopyOrMoveSource, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
	if err != nil {
		return si, di, errors.Wrap(err, "invalid source")
	}

	if snapshotCopyOrMoveDestination == "" {
		// no destination - assume current user@hostname
		di.UserName = rep.ClientOptions().Username
		di.Host = rep.ClientOptions().Hostname
	} else {
		di, err = snapshot.ParseSourceInfo(snapshotCopyOrMoveDestination, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
		if err != nil {
			return si, di, errors.Wrap(err, "invalid destination")
		}
	}

	if di.Path != "" && si.Path == "" {
		// it is illegal to specify source without path, but destination with a path
		// as it would result in multiple individual paths being squished together.
		return si, di, errors.Errorf("path specified on destination but not source")
	}

	if di.UserName != "" && si.UserName == "" {
		// it is illegal to specify source without username, but destination with a username
		// as it would result in multiple individual paths being squished together.
		return si, di, errors.Errorf("username specified on destination but not source")
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

func init() {
	registerSnapshotCopyFlags(snapshotCopyCommand)

	snapshotCopyCommand.Action(repositoryAction(func(ctx context.Context, rep repo.Repository) error {
		return runSnapshotCopyCommand(ctx, rep, false)
	}))

	registerSnapshotCopyFlags(snapshotMoveCommand)
	snapshotMoveCommand.Action(repositoryAction(func(ctx context.Context, rep repo.Repository) error {
		return runSnapshotCopyCommand(ctx, rep, true)
	}))
}
