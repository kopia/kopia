package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

var (
	snapshotListCommand           = snapshotCommands.Command("list", "List snapshots of files and directories.").Alias("ls")
	snapshotListPath              = snapshotListCommand.Arg("source", "File or directory to show history of.").String()
	snapshotListIncludeIncomplete = snapshotListCommand.Flag("include-incomplete", "Include incomplete.").Short('i').Bool()
	snapshotListShowItemID        = snapshotListCommand.Flag("show-metadata-id", "Include metadata item ID.").Short('m').Bool()
	snapshotListShowHashCache     = snapshotListCommand.Flag("show-hashcache", "Include hashcache object ID.").Bool()
	maxResultsPerPath             = snapshotListCommand.Flag("max-results", "Maximum number of results.").Default("1000").Int()
)

func findSnapshotsForSource(mgr *snapshot.Manager, sourceInfo snapshot.SourceInfo) (manifestIDs []string, relPath string, err error) {
	for len(sourceInfo.Path) > 0 {
		list := mgr.ListSnapshotManifests(&sourceInfo)

		if len(list) > 0 {
			return list, relPath, nil
		}

		if len(relPath) > 0 {
			relPath = filepath.Base(sourceInfo.Path) + "/" + relPath
		} else {
			relPath = filepath.Base(sourceInfo.Path)
		}

		log.Printf("No snapshots of %v@%v:%v", sourceInfo.UserName, sourceInfo.Host, sourceInfo.Path)

		parentPath := filepath.Dir(sourceInfo.Path)
		if parentPath == sourceInfo.Path {
			break
		}
		sourceInfo.Path = parentPath
	}

	return nil, "", nil
}

func findManifestIDs(mgr *snapshot.Manager, source string) ([]string, string, error) {
	if source == "" {
		return mgr.ListSnapshotManifests(nil), "", nil
	}

	si, err := snapshot.ParseSourceInfo(source, getHostName(), getUserName())
	if err != nil {
		return nil, "", fmt.Errorf("invalid directory: '%s': %s", source, err)
	}

	manifestIDs, relPath, err := findSnapshotsForSource(mgr, si)
	if relPath != "" {
		relPath = "/" + relPath
	}

	return manifestIDs, relPath, err
}

func runBackupsCommand(ctx context.Context, rep *repo.Repository) error {
	mgr := snapshot.NewManager(rep)

	manifestIDs, relPath, err := findManifestIDs(mgr, *snapshotListPath)
	if err != nil {
		return err
	}

	manifests, err := mgr.LoadSnapshots(manifestIDs)
	if err != nil {
		return err
	}

	polMgr := snapshot.NewPolicyManager(rep)

	outputManifestGroups(manifests, relPath, polMgr)

	return nil
}

func outputManifestGroups(manifests []*snapshot.Manifest, relPath string, polMgr *snapshot.PolicyManager) {
	separator := ""
	for _, snapshotGroup := range snapshot.GroupBySource(manifests) {
		src := snapshotGroup[0].Source
		fmt.Printf("%v%v\n", separator, src)
		separator = "\n"

		pol, err := polMgr.GetEffectivePolicy(src)
		if err != nil {
			log.Warn().Msgf("unable to determine effective policy for %v", src)
		} else {
			pol.RetentionPolicy.ComputeRetentionReasons(snapshotGroup)
		}
		outputManifestFromSingleSource(snapshotGroup, relPath)
	}
}

func outputManifestFromSingleSource(manifests []*snapshot.Manifest, relPath string) {
	var count int
	var lastTotalFileSize int64

	for _, m := range snapshot.SortByTime(manifests, false) {
		maybeIncomplete := ""
		if m.IncompleteReason != "" {
			if !*snapshotListIncludeIncomplete {
				continue
			}
			maybeIncomplete = " " + m.IncompleteReason
		}

		if count > *maxResultsPerPath {
			return
		}

		fmt.Printf(
			"  %v %v%v %v %v %v %v\n",
			m.StartTime.Format("2006-01-02 15:04:05 MST"),
			m.RootObjectID,
			relPath,
			units.BytesStringBase10(m.Stats.TotalFileSize),
			retentionReasonString(m.RetentionReasons),
			deltaBytes(m.Stats.TotalFileSize-lastTotalFileSize),
			maybeIncomplete,
		)

		if *snapshotListShowItemID {
			fmt.Printf("    metadata:  %v\n", m.ID)
		}
		if *snapshotListShowHashCache {
			fmt.Printf("    hashcache: %v\n", m.HashCacheID)
		}
		count++

		if m.IncompleteReason == "" || !*snapshotListIncludeIncomplete {
			lastTotalFileSize = m.Stats.TotalFileSize
		}
	}
}

func deltaBytes(b int64) string {
	if b > 0 {
		return "(+" + units.BytesStringBase10(b) + ")"
	}

	return ""
}

func retentionReasonString(s []string) string {
	if len(s) == 0 {
		return "-"
	}
	return strings.Join(s, ",")
}

func init() {
	snapshotListCommand.Action(repositoryAction(runBackupsCommand))
}
