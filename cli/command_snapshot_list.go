package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
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

	sort.Sort(manifestSorter(manifests))
	outputManifests(manifests, relPath)

	return nil
}

func outputManifests(manifests []*snapshot.Manifest, relPath string) {
	var lastSource snapshot.SourceInfo
	var count int
	var lastTotalFileSize int64

	separator := ""

	for _, m := range manifests {
		maybeIncomplete := ""
		if m.IncompleteReason != "" {
			if !*snapshotListIncludeIncomplete {
				continue
			}
			maybeIncomplete = " " + m.IncompleteReason
		}

		if m.Source != lastSource {
			fmt.Printf("%v%v\n", separator, m.Source)
			separator = "\n"
			lastSource = m.Source
			count = 0
			lastTotalFileSize = m.Stats.TotalFileSize
		}

		if count < *maxResultsPerPath {
			fmt.Printf(
				"  %v %v%v %v %v%v\n",
				m.StartTime.Format("2006-01-02 15:04:05 MST"),
				m.RootObjectID,
				relPath,
				units.BytesStringBase10(m.Stats.TotalFileSize),
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
		}

		if m.IncompleteReason == "" || !*snapshotListIncludeIncomplete {
			lastTotalFileSize = m.Stats.TotalFileSize
		}
	}
}

type manifestSorter []*snapshot.Manifest

func (b manifestSorter) Len() int { return len(b) }
func (b manifestSorter) Less(i, j int) bool {
	if c := strings.Compare(b[i].Source.String(), b[j].Source.String()); c != 0 {
		return c < 0
	}

	return b[i].StartTime.UnixNano() < b[j].StartTime.UnixNano()
}

func (b manifestSorter) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

func deltaBytes(b int64) string {
	if b > 0 {
		return "(+" + units.BytesStringBase10(b) + ")"
	}

	return ""
}

func init() {
	snapshotListCommand.Action(repositoryAction(runBackupsCommand))
}
