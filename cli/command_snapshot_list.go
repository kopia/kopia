package cli

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/snapshot"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	snapshotListCommand           = snapshotCommands.Command("list", "List snapshots of files and directories.").Alias("ls")
	snapshotListPath              = snapshotListCommand.Arg("source", "File or directory to show history of.").String()
	snapshotListIncludeIncomplete = snapshotListCommand.Flag("include-incomplete", "Include incomplete.").Short('i').Bool()
	snapshotListShowItemID        = snapshotListCommand.Flag("show-metadata-id", "Include metadata item ID.").Short('m').Bool()
	snapshotListShowHashCache     = snapshotListCommand.Flag("show-hashcache", "Include hashcache object ID.").Bool()
	maxResultsPerPath             = snapshotListCommand.Flag("max-results", "Maximum number of results.").Default("1000").Int()
)

func findBackups(mgr *snapshot.Manager, sourceInfo snapshot.SourceInfo) (manifestIDs []string, relPath string, err error) {
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

func runBackupsCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	mgr := snapshot.NewManager(rep)

	var previous []string
	var relPath string
	var err error

	if *snapshotListPath != "" {
		si, err := snapshot.ParseSourceInfo(*snapshotListPath, getHostName(), getUserName())
		if err != nil {
			return fmt.Errorf("invalid directory: '%s': %s", *snapshotListPath, err)
		}

		previous, relPath, err = findBackups(mgr, si)
		if relPath != "" {
			relPath = "/" + relPath
		}
	} else {
		previous = mgr.ListSnapshotManifests(nil)
	}

	if err != nil {
		return fmt.Errorf("cannot list snapshots: %v", err)
	}

	var lastSource snapshot.SourceInfo
	var count int

	manifestToItemID := map[*snapshot.Manifest]string{}

	manifests, err := mgr.LoadSnapshots(previous)
	if err != nil {
		return err
	}

	for i, m := range manifests {
		manifestToItemID[m] = previous[i]
	}

	sort.Sort(manifestSorter(manifests))

	var lastTotalFileSize int64
	for _, m := range manifests {
		maybeIncomplete := ""
		if m.IncompleteReason != "" {
			if !*snapshotListIncludeIncomplete {
				continue
			}
			maybeIncomplete = " " + m.IncompleteReason
		}

		if m.Source != lastSource {
			fmt.Printf("\n%v\n", m.Source)
			lastSource = m.Source
			count = 0
			lastTotalFileSize = m.Stats.TotalFileSize
		}

		if count < *maxResultsPerPath {
			fmt.Printf(
				"  %v%v %v %10v %v%v\n",
				m.RootObjectID,
				relPath,
				m.StartTime.Format("2006-01-02 15:04:05 MST"),
				units.BytesStringBase10(m.Stats.TotalFileSize),
				deltaBytes(m.Stats.TotalFileSize-lastTotalFileSize),
				maybeIncomplete,
			)
			if *snapshotListShowItemID {
				if i, ok := manifestToItemID[m]; ok {
					fmt.Printf("    metadata:  %v\n", i)
				}
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

	return nil
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
	snapshotListCommand.Action(runBackupsCommand)
}
