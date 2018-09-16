package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/repofs"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/policy"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

var (
	snapshotListCommand              = snapshotCommands.Command("list", "List snapshots of files and directories.").Alias("ls")
	snapshotListPath                 = snapshotListCommand.Arg("source", "File or directory to show history of.").String()
	snapshotListIncludeIncomplete    = snapshotListCommand.Flag("incomplete", "Include incomplete.").Short('i').Bool()
	snapshotListShowHumanReadable    = snapshotListCommand.Flag("human-readable", "Show human-readable units").Default("true").Bool()
	snapshotListShowDelta            = snapshotListCommand.Flag("delta", "Include deltas.").Short('d').Bool()
	snapshotListShowItemID           = snapshotListCommand.Flag("manifest-id", "Include manifest item ID.").Short('m').Bool()
	snapshotListShowHashCache        = snapshotListCommand.Flag("hashcache", "Include hashcache object ID.").Bool()
	snapshotListShowRetentionReasons = snapshotListCommand.Flag("retention", "Include retention reasons.").Default("true").Bool()
	snapshotListShowModTime          = snapshotListCommand.Flag("mtime", "Include file mod time").Bool()
	shapshotListShowOwner            = snapshotListCommand.Flag("owner", "Include owner").Bool()
	maxResultsPerPath                = snapshotListCommand.Flag("max-results", "Maximum number of results.").Default("1000").Int()
)

func findSnapshotsForSource(ctx context.Context, rep *repo.Repository, sourceInfo snapshot.SourceInfo) (manifestIDs []string, relPath string, err error) {
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

		log.Debugf("No snapshots of %v@%v:%v", sourceInfo.UserName, sourceInfo.Host, sourceInfo.Path)

		parentPath := filepath.Dir(sourceInfo.Path)
		if parentPath == sourceInfo.Path {
			break
		}
		sourceInfo.Path = parentPath
	}

	return nil, "", nil
}

func findManifestIDs(ctx context.Context, rep *repo.Repository, source string) ([]string, string, error) {
	if source == "" {
		man, err := snapshot.ListSnapshotManifests(ctx, rep, nil)
		return man, "", err
	}

	si, err := snapshot.ParseSourceInfo(source, getHostName(), getUserName())
	if err != nil {
		return nil, "", fmt.Errorf("invalid directory: '%s': %s", source, err)
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

func outputManifestGroups(ctx context.Context, rep *repo.Repository, manifests []*snapshot.Manifest, relPathParts []string) error {
	separator := ""
	for _, snapshotGroup := range snapshot.GroupBySource(manifests) {
		src := snapshotGroup[0].Source
		fmt.Printf("%v%v\n", separator, src)
		separator = "\n"

		pol, _, err := policy.GetEffectivePolicy(ctx, rep, src)
		if err != nil {
			log.Warningf("unable to determine effective policy for %v", src)
		} else {
			pol.RetentionPolicy.ComputeRetentionReasons(snapshotGroup)
		}
		if err := outputManifestFromSingleSource(ctx, rep, snapshotGroup, relPathParts); err != nil {
			return err
		}
	}

	return nil
}

//nolint:gocyclo
func outputManifestFromSingleSource(ctx context.Context, rep *repo.Repository, manifests []*snapshot.Manifest, parts []string) error {
	var count int
	var lastTotalFileSize int64

	manifests = snapshot.SortByTime(manifests, false)
	if len(manifests) > *maxResultsPerPath {
		manifests = manifests[len(manifests)-*maxResultsPerPath:]
	}

	for _, m := range manifests {
		root, err := repofs.SnapshotRoot(rep, m)
		if err != nil {
			fmt.Printf("  %v <ERROR> %v\n", m.StartTime.Format("2006-01-02 15:04:05 MST"), err)
			continue
		}
		ent, err := getNestedEntry(ctx, root, parts)
		if err != nil {
			fmt.Printf("  %v <ERROR> %v\n", m.StartTime.Format("2006-01-02 15:04:05 MST"), err)
			continue
		}

		if _, ok := ent.(object.HasObjectID); !ok {
			log.Warningf("entry does not have object ID: %v", ent, err)
			continue
		}

		var bits []string
		if m.IncompleteReason != "" {
			if !*snapshotListIncludeIncomplete {
				continue
			}
			bits = append(bits, "incomplete:"+m.IncompleteReason)
		}

		bits = append(bits, maybeHumanReadableBytes(*snapshotListShowHumanReadable, ent.Metadata().FileSize))
		bits = append(bits, fmt.Sprintf("%v", ent.Metadata().FileMode()))
		if *shapshotListShowOwner {
			bits = append(bits, fmt.Sprintf("uid:%v", ent.Metadata().UserID))
			bits = append(bits, fmt.Sprintf("gid:%v", ent.Metadata().GroupID))
		}
		if *snapshotListShowModTime {
			bits = append(bits, fmt.Sprintf("modified:%v", ent.Metadata().ModTime.Format(timeFormat)))
		}

		if *snapshotListShowItemID {
			bits = append(bits, "manifest:"+m.ID)
		}
		if *snapshotListShowHashCache {
			bits = append(bits, "hashcache:"+m.HashCacheID.String())
		}

		if *snapshotListShowDelta {
			bits = append(bits, deltaBytes(ent.Metadata().FileSize-lastTotalFileSize))
		}

		if d, ok := ent.(fs.Directory); ok {
			s := d.Summary()
			if s != nil {
				bits = append(bits, fmt.Sprintf("files:%v", s.TotalFileCount))
				bits = append(bits, fmt.Sprintf("dirs:%v", s.TotalDirCount))
			}
		}

		if *snapshotListShowRetentionReasons {
			if len(m.RetentionReasons) > 0 {
				bits = append(bits, "retention:"+strings.Join(m.RetentionReasons, ","))
			}
		}

		fmt.Printf(
			"  %v %v %v\n",
			m.StartTime.Format("2006-01-02 15:04:05 MST"),
			ent.(object.HasObjectID).ObjectID(),
			strings.Join(bits, " "),
		)

		count++
		if m.IncompleteReason == "" {
			lastTotalFileSize = m.Stats.TotalFileSize
		}
	}

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
