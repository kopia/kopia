package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var (
	snapshotListCommand              = snapshotCommands.Command("list", "List snapshots of files and directories.").Alias("ls")
	snapshotListPath                 = snapshotListCommand.Arg("source", "File or directory to show history of.").String()
	snapshotListIncludeIncomplete    = snapshotListCommand.Flag("incomplete", "Include incomplete.").Short('i').Bool()
	snapshotListShowHumanReadable    = snapshotListCommand.Flag("human-readable", "Show human-readable units").Default("true").Bool()
	snapshotListShowDelta            = snapshotListCommand.Flag("delta", "Include deltas.").Short('d').Bool()
	snapshotListShowItemID           = snapshotListCommand.Flag("manifest-id", "Include manifest item ID.").Short('m').Bool()
	snapshotListShowRetentionReasons = snapshotListCommand.Flag("retention", "Include retention reasons.").Default("true").Bool()
	snapshotListShowModTime          = snapshotListCommand.Flag("mtime", "Include file mod time").Bool()
	shapshotListShowOwner            = snapshotListCommand.Flag("owner", "Include owner").Bool()
	snapshotListShowIdentical        = snapshotListCommand.Flag("show-identical", "Show identical snapshots").Short('l').Bool()
	snapshotListShowAll              = snapshotListCommand.Flag("all", "Show all shapshots (not just current username/host)").Short('a').Bool()
	maxResultsPerPath                = snapshotListCommand.Flag("max-results", "Maximum number of entries per source.").Default("100").Short('n').Int()
)

func findSnapshotsForSource(ctx context.Context, rep *repo.Repository, sourceInfo snapshot.SourceInfo) (manifestIDs []manifest.ID, relPath string, err error) {
	for len(sourceInfo.Path) > 0 {
		list, err := snapshot.ListSnapshotManifests(ctx, rep, &sourceInfo)
		if err != nil {
			return nil, "", err
		}

		if len(list) > 0 {
			return list, relPath, nil
		}

		if len(relPath) > 0 {
			relPath = filepath.Base(sourceInfo.Path) + "/" + relPath
		} else {
			relPath = filepath.Base(sourceInfo.Path)
		}

		log(ctx).Debugf("No snapshots of %v@%v:%v", sourceInfo.UserName, sourceInfo.Host, sourceInfo.Path)

		parentPath := filepath.Dir(sourceInfo.Path)
		if parentPath == sourceInfo.Path {
			break
		}

		sourceInfo.Path = parentPath
	}

	return nil, "", nil
}

func findManifestIDs(ctx context.Context, rep *repo.Repository, source string) ([]manifest.ID, string, error) {
	if source == "" {
		man, err := snapshot.ListSnapshotManifests(ctx, rep, nil)
		return man, "", err
	}

	si, err := snapshot.ParseSourceInfo(source, rep.Hostname, rep.Username)
	if err != nil {
		return nil, "", errors.Errorf("invalid directory: '%s': %s", source, err)
	}

	manifestIDs, relPath, err := findSnapshotsForSource(ctx, rep, si)
	if relPath != "" {
		relPath = "/" + relPath
	}

	return manifestIDs, relPath, err
}

func runSnapshotsCommand(ctx context.Context, rep *repo.Repository) error {
	manifestIDs, relPath, err := findManifestIDs(ctx, rep, *snapshotListPath)
	if err != nil {
		return err
	}

	manifests, err := snapshot.LoadSnapshots(ctx, rep, manifestIDs)
	if err != nil {
		return err
	}

	return outputManifestGroups(ctx, rep, manifests, strings.Split(relPath, "/"))
}

func shouldOutputSnapshotSource(rep *repo.Repository, src snapshot.SourceInfo) bool {
	if *snapshotListShowAll {
		return true
	}

	if src.Host != rep.Hostname {
		return false
	}

	return src.UserName == rep.Username
}

func outputManifestGroups(ctx context.Context, rep *repo.Repository, manifests []*snapshot.Manifest, relPathParts []string) error {
	separator := ""

	var anyOutput bool

	for _, snapshotGroup := range snapshot.GroupBySource(manifests) {
		src := snapshotGroup[0].Source
		if !shouldOutputSnapshotSource(rep, src) {
			log(ctx).Debugf("skipping %v", src)
			continue
		}

		fmt.Printf("%v%v\n", separator, src)

		separator = "\n"
		anyOutput = true

		pol, _, err := policy.GetEffectivePolicy(ctx, rep, src)
		if err != nil {
			log(ctx).Warningf("unable to determine effective policy for %v", src)
		} else {
			pol.RetentionPolicy.ComputeRetentionReasons(snapshotGroup)
		}

		if err := outputManifestFromSingleSource(ctx, rep, snapshotGroup, relPathParts); err != nil {
			return err
		}
	}

	if !anyOutput && !*snapshotListShowAll && len(manifests) > 0 {
		printStderr("No snapshots found. Pass --all to show snapshots from all users/hosts.\n")
	}

	return nil
}

//nolint:gocyclo,funlen
func outputManifestFromSingleSource(ctx context.Context, rep *repo.Repository, manifests []*snapshot.Manifest, parts []string) error {
	var count int

	var lastTotalFileSize int64

	manifests = snapshot.SortByTime(manifests, false)
	if len(manifests) > *maxResultsPerPath {
		manifests = manifests[len(manifests)-*maxResultsPerPath:]
	}

	var previousOID object.ID

	var elidedCount int

	var maxElidedTime time.Time

	outputElided := func() {
		if elidedCount > 0 {
			fmt.Printf(
				"  + %v identical snapshots until %v\n",
				elidedCount,
				formatTimestamp(maxElidedTime),
			)
		}
	}

	for _, m := range manifests {
		root, err := snapshotfs.SnapshotRoot(rep, m)
		if err != nil {
			fmt.Printf("  %v <ERROR> %v\n", formatTimestamp(m.StartTime), err)
			continue
		}

		ent, err := getNestedEntry(ctx, root, parts)
		if err != nil {
			fmt.Printf("  %v <ERROR> %v\n", formatTimestamp(m.StartTime), err)
			continue
		}

		if _, ok := ent.(object.HasObjectID); !ok {
			log(ctx).Warningf("entry does not have object ID: %v", ent, err)
			continue
		}

		var bits []string

		if m.IncompleteReason != "" {
			if !*snapshotListIncludeIncomplete {
				continue
			}

			bits = append(bits, "incomplete:"+m.IncompleteReason)
		}

		bits = append(bits,
			maybeHumanReadableBytes(*snapshotListShowHumanReadable, ent.Size()),
			fmt.Sprintf("%v", ent.Mode()))
		if *shapshotListShowOwner {
			bits = append(bits,
				fmt.Sprintf("uid:%v", ent.Owner().UserID),
				fmt.Sprintf("gid:%v", ent.Owner().GroupID))
		}

		if *snapshotListShowModTime {
			bits = append(bits, fmt.Sprintf("modified:%v", formatTimestamp(ent.ModTime())))
		}

		if *snapshotListShowItemID {
			bits = append(bits, "manifest:"+string(m.ID))
		}

		if *snapshotListShowDelta {
			bits = append(bits, deltaBytes(ent.Size()-lastTotalFileSize))
		}

		if d, ok := ent.(fs.Directory); ok {
			s := d.Summary()
			if s != nil {
				bits = append(bits,
					fmt.Sprintf("files:%v", s.TotalFileCount),
					fmt.Sprintf("dirs:%v", s.TotalDirCount))
			}
		}

		if *snapshotListShowRetentionReasons {
			if len(m.RetentionReasons) > 0 {
				bits = append(bits, "("+strings.Join(m.RetentionReasons, ",")+")")
			}
		}

		oid := ent.(object.HasObjectID).ObjectID()
		if !*snapshotListShowIdentical && oid == previousOID {
			elidedCount++

			maxElidedTime = m.StartTime

			continue
		}

		previousOID = oid

		outputElided()

		elidedCount = 0

		fmt.Printf(
			"  %v %v %v\n",
			formatTimestamp(m.StartTime),
			oid,
			strings.Join(bits, " "),
		)

		count++

		if m.IncompleteReason == "" {
			lastTotalFileSize = m.Stats.TotalFileSize
		}
	}

	outputElided()

	return nil
}

func deltaBytes(b int64) string {
	if b > 0 {
		return "(+" + units.BytesStringBase10(b) + ")"
	}

	return ""
}

func init() {
	snapshotListCommand.Action(repositoryAction(runSnapshotsCommand))
}
