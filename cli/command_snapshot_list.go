package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/fatih/color"
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

type commandSnapshotList struct {
	snapshotListPath                 string
	snapshotListIncludeIncomplete    bool
	snapshotListShowHumanReadable    bool
	snapshotListShowDelta            bool
	snapshotListShowItemID           bool
	snapshotListShowRetentionReasons bool
	snapshotListShowModTime          bool
	shapshotListShowOwner            bool
	snapshotListShowIdentical        bool
	snapshotListShowAll              bool
	maxResultsPerPath                int
	snapshotListTags                 []string
	storageStats                     bool
	reverseSort                      bool

	jo  jsonOutput
	out textOutput
}

func (c *commandSnapshotList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List snapshots of files and directories.").Alias("ls")
	cmd.Arg("source", "File or directory to show history of.").StringVar(&c.snapshotListPath)
	cmd.Flag("incomplete", "Include incomplete.").Short('i').BoolVar(&c.snapshotListIncludeIncomplete)
	cmd.Flag("human-readable", "Show human-readable units").Default("true").BoolVar(&c.snapshotListShowHumanReadable)
	cmd.Flag("delta", "Include deltas.").Short('d').BoolVar(&c.snapshotListShowDelta)
	cmd.Flag("manifest-id", "Include manifest item ID.").Short('m').BoolVar(&c.snapshotListShowItemID)
	cmd.Flag("retention", "Include retention reasons.").Default("true").BoolVar(&c.snapshotListShowRetentionReasons)
	cmd.Flag("mtime", "Include file mod time").BoolVar(&c.snapshotListShowModTime)
	cmd.Flag("owner", "Include owner").BoolVar(&c.shapshotListShowOwner)
	cmd.Flag("show-identical", "Show identical snapshots").Short('l').BoolVar(&c.snapshotListShowIdentical)
	cmd.Flag("storage-stats", "Compute and show storage statistics").BoolVar(&c.storageStats)
	cmd.Flag("reverse", "Reverse sort order").BoolVar(&c.reverseSort)
	cmd.Flag("all", "Show all snapshots (not just current username/host)").Short('a').BoolVar(&c.snapshotListShowAll)
	cmd.Flag("max-results", "Maximum number of entries per source.").Short('n').IntVar(&c.maxResultsPerPath)
	cmd.Flag("tags", "Tag filters to apply on the list items. Must be provided in the <key>:<value> format.").StringsVar(&c.snapshotListTags)
	c.jo.setup(svc, cmd)
	c.out.setup(svc)
	cmd.Action(svc.repositoryReaderAction(c.run))
}

func findSnapshotsForSource(ctx context.Context, rep repo.Repository, sourceInfo snapshot.SourceInfo, tags map[string]string) (manifestIDs []manifest.ID, err error) {
	var result []manifest.ID

	for sourceInfo.Path != "" {
		list, err := snapshot.ListSnapshotManifests(ctx, rep, &sourceInfo, tags)
		if err != nil {
			return nil, errors.Wrapf(err, "error listing manifests for %v", sourceInfo)
		}

		if len(list) > 0 {
			result = append(result, list...)
		}

		parentPath := filepath.Dir(sourceInfo.Path)
		if parentPath == sourceInfo.Path {
			break
		}

		sourceInfo.Path = parentPath
	}

	return result, nil
}

func findRelativePathParts(m *snapshot.Manifest, path string) ([]string, error) {
	if path == "" {
		return nil, nil
	}

	relPath, err := filepath.Rel(m.Source.Path, path)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	if relPath == "." {
		return nil, nil
	}

	return strings.Split(filepath.ToSlash(relPath), "/"), nil
}

func findManifestIDs(ctx context.Context, rep repo.Repository, source string, tags map[string]string) ([]manifest.ID, string, error) {
	if source == "" {
		man, err := snapshot.ListSnapshotManifests(ctx, rep, nil, tags)
		return man, "", errors.Wrap(err, "error listing all snapshot manifests")
	}

	si, err := snapshot.ParseSourceInfo(source, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
	if err != nil {
		return nil, "", errors.Errorf("invalid directory: '%s': %s", source, err)
	}

	manifestIDs, err := findSnapshotsForSource(ctx, rep, si, tags)

	return manifestIDs, si.Path, err
}

func (c *commandSnapshotList) run(ctx context.Context, rep repo.Repository) error {
	tags, err := getTags(c.snapshotListTags)
	if err != nil {
		return err
	}

	manifestIDs, fullPath, err := findManifestIDs(ctx, rep, c.snapshotListPath, tags)
	if err != nil {
		return err
	}

	manifests, err := snapshot.LoadSnapshots(ctx, rep, manifestIDs)
	if err != nil {
		return errors.Wrap(err, "unable to load snapshots")
	}

	if c.jo.jsonOutput {
		return c.outputJSON(ctx, rep, manifests)
	}

	return c.outputManifestGroups(ctx, rep, manifests, fullPath)
}

// SnapshotManifest defines the JSON output for the CLI snapshot commands.
type SnapshotManifest struct {
	*snapshot.Manifest
	RetentionReasons []string `json:"retentionReason,omitempty"`
}

func (c *commandSnapshotList) outputJSON(ctx context.Context, rep repo.Repository, manifests []*snapshot.Manifest) error {
	var jl jsonList

	jl.begin(&c.jo)
	defer jl.end()

	for _, snapshotGroup := range snapshot.GroupBySource(manifests) {
		snapshotGroup = snapshot.SortByTime(snapshotGroup, c.reverseSort)

		if c.maxResultsPerPath > 0 && len(snapshotGroup) > c.maxResultsPerPath {
			snapshotGroup = snapshotGroup[len(snapshotGroup)-c.maxResultsPerPath:]
		}

		if c.snapshotListShowRetentionReasons {
			src := snapshotGroup[0].Source
			// compute retention reason
			pol, _, _, err := policy.GetEffectivePolicy(ctx, rep, src)
			if err != nil {
				log(ctx).Errorf("unable to determine effective policy for %v", src)
			} else {
				pol.RetentionPolicy.ComputeRetentionReasons(snapshotGroup)
			}
		}

		if err := c.iterateSnapshotsMaybeWithStorageStats(ctx, rep, snapshotGroup, func(m *snapshot.Manifest) error {
			wm := SnapshotManifest{Manifest: m, RetentionReasons: m.RetentionReasons}
			jl.emit(wm)
			return nil
		}); err != nil {
			return errors.Wrap(err, "unable to iterate snapshots")
		}
	}

	return nil
}

func (c *commandSnapshotList) shouldOutputSnapshotSource(rep repo.Repository, src snapshot.SourceInfo) bool {
	if c.snapshotListShowAll {
		return true
	}

	co := rep.ClientOptions()

	if src.Host != co.Hostname {
		return false
	}

	return src.UserName == co.Username
}

func (c *commandSnapshotList) outputManifestGroups(ctx context.Context, rep repo.Repository, manifests []*snapshot.Manifest, path string) error {
	separator := ""

	var anyOutput bool

	for _, snapshotGroup := range snapshot.GroupBySource(manifests) {
		src := snapshotGroup[0].Source
		if !c.shouldOutputSnapshotSource(rep, src) {
			log(ctx).Debugf("skipping %v", src)
			continue
		}

		c.out.printStdout("%v%v\n", separator, src)

		separator = "\n"
		anyOutput = true

		pol, _, _, err := policy.GetEffectivePolicy(ctx, rep, src)
		if err != nil {
			log(ctx).Errorf("unable to determine effective policy for %v", src)
		} else {
			pol.RetentionPolicy.ComputeRetentionReasons(snapshotGroup)
		}

		relPathParts, err := findRelativePathParts(snapshotGroup[0], path)
		if err != nil {
			return err
		}

		if err := c.outputManifestFromSingleSource(ctx, rep, snapshotGroup, relPathParts); err != nil {
			return err
		}
	}

	if !anyOutput && !c.snapshotListShowAll && len(manifests) > 0 {
		log(ctx).Info("No snapshots found. Pass --all to show snapshots from all users/hosts.\n")
	}

	return nil
}

type snapshotListRow struct {
	firstStartTime   time.Time
	lastStartTime    time.Time
	count            int
	oid              object.ID
	bits             []string
	retentionReasons []string
	pins             []string
	color            *color.Color
}

func (c *commandSnapshotList) iterateSnapshotsMaybeWithStorageStats(ctx context.Context, rep repo.Repository, manifests []*snapshot.Manifest, callback func(m *snapshot.Manifest) error) error {
	if c.storageStats {
		//nolint:wrapcheck
		return snapshotfs.CalculateStorageStats(ctx, rep, manifests, callback)
	}

	for _, m := range manifests {
		if err := callback(m); err != nil {
			return err
		}
	}

	return nil
}

func (c *commandSnapshotList) outputManifestFromSingleSource(ctx context.Context, rep repo.Repository, manifests []*snapshot.Manifest, parts []string) error {
	var lastTotalFileSize int64

	manifests = snapshot.SortByTime(manifests, c.reverseSort)
	if c.maxResultsPerPath > 0 && len(manifests) > c.maxResultsPerPath {
		manifests = manifests[len(manifests)-c.maxResultsPerPath:]
	}

	var rows []*snapshotListRow

	if err := c.iterateSnapshotsMaybeWithStorageStats(ctx, rep, manifests, func(m *snapshot.Manifest) error {
		root, err := snapshotfs.SnapshotRoot(rep, m)
		if err != nil {
			c.out.printStdout("  %v <ERROR> %v\n", formatTimestamp(m.StartTime.ToTime()), err)
			return nil
		}

		ent, err := snapshotfs.GetNestedEntry(ctx, root, parts)
		if err != nil {
			c.out.printStdout("  %v <ERROR> %v\n", formatTimestamp(m.StartTime.ToTime()), err)
			return nil
		}

		ohid, ok := ent.(object.HasObjectID)
		if !ok {
			log(ctx).Errorf("entry for '%s' does not have object ID: %v", ent.Name(), err)
			return nil
		}

		if m.IncompleteReason != "" && !c.snapshotListIncludeIncomplete {
			return nil
		}

		bits, col := c.entryBits(ctx, m, ent, lastTotalFileSize)

		rows = append(rows, &snapshotListRow{
			firstStartTime:   m.StartTime.ToTime(),
			lastStartTime:    m.StartTime.ToTime(),
			count:            1,
			oid:              ohid.ObjectID(),
			bits:             bits,
			retentionReasons: m.RetentionReasons,
			pins:             m.Pins,
			color:            col,
		})

		if m.IncompleteReason == "" {
			lastTotalFileSize = atomic.LoadInt64(&m.Stats.TotalFileSize)
		}

		return nil
	}); err != nil {
		return err
	}

	if !c.snapshotListShowIdentical {
		rows = c.mergeIdenticalRows(rows)
	}

	c.outputSnapshotRows(rows)

	return nil
}

func (c *commandSnapshotList) mergeIdenticalRows(rows []*snapshotListRow) []*snapshotListRow {
	var result []*snapshotListRow

	for _, r := range rows {
		if len(result) == 0 {
			result = append(result, r)
			continue
		}

		last := result[len(result)-1]

		if r.oid == last.oid {
			last.count++
			last.lastStartTime = r.lastStartTime
			last.retentionReasons = append(last.retentionReasons, r.retentionReasons...)
			last.pins = append(last.pins, r.pins...)
		} else {
			result = append(result, r)
		}
	}

	for _, r := range result {
		r.retentionReasons = policy.CompactRetentionReasons(r.retentionReasons)
		r.pins = policy.CompactPins(r.pins)
	}

	return result
}

func (c *commandSnapshotList) outputSnapshotRows(rows []*snapshotListRow) {
	for _, row := range rows {
		bits := append([]string(nil), row.bits...)

		if c.snapshotListShowRetentionReasons {
			if len(row.retentionReasons) > 0 {
				bits = append(bits, "("+strings.Join(row.retentionReasons, ",")+")")
			}
		}

		if len(row.pins) > 0 {
			bits = append(bits, "pins:"+strings.Join(row.pins, ","))
		}

		row.color.Fprint(c.out.stdout(), fmt.Sprintf("  %v %v %v\n", formatTimestamp(row.firstStartTime), row.oid, strings.Join(bits, " "))) //nolint:errcheck

		if row.count > 1 {
			c.out.printStdout(
				"  + %v identical snapshots until %v\n",
				row.count-1,
				formatTimestamp(row.lastStartTime),
			)
		}
	}
}

func (c *commandSnapshotList) entryBits(ctx context.Context, m *snapshot.Manifest, ent fs.Entry, lastTotalFileSize int64) (bits []string, col *color.Color) {
	col = color.New() // default color

	if m.IncompleteReason != "" {
		bits = append(bits, "incomplete:"+m.IncompleteReason)
	}

	var summary *fs.DirectorySummary

	if dws, ok := ent.(fs.DirectoryWithSummary); ok {
		s, err := dws.Summary(ctx)
		if err != nil {
			log(ctx).Warnw("unable to get directory summary", "name", ent.Name(), "err", err)
		}

		summary = s
	}

	totalBytes := ent.Size()
	if summary != nil {
		totalBytes = summary.TotalFileSize
	}

	bits = append(bits,
		maybeHumanReadableBytes(c.snapshotListShowHumanReadable, totalBytes),
		ent.Mode().String())
	if c.shapshotListShowOwner {
		bits = append(bits,
			fmt.Sprintf("uid:%v", ent.Owner().UserID),
			fmt.Sprintf("gid:%v", ent.Owner().GroupID))
	}

	if c.snapshotListShowModTime {
		bits = append(bits, fmt.Sprintf("modified:%v", formatTimestamp(ent.ModTime())))
	}

	if c.snapshotListShowItemID {
		bits = append(bits, "manifest:"+string(m.ID))
	}

	if c.snapshotListShowDelta {
		bits = append(bits, deltaBytes(ent.Size()-lastTotalFileSize))
	}

	if summary != nil {
		bits = append(bits,
			fmt.Sprintf("files:%v", summary.TotalFileCount),
			fmt.Sprintf("dirs:%v", summary.TotalDirCount))
		if summary.FatalErrorCount > 0 {
			bits = append(bits, fmt.Sprintf("errors:%v", summary.FatalErrorCount))
			col = errorColor
		}
	}

	if u := m.StorageStats; u != nil {
		bits = append(bits,
			fmt.Sprintf("new-data:%v", units.BytesString(atomic.LoadInt64(&u.NewData.PackedContentBytes))),
			fmt.Sprintf("new-files:%v", atomic.LoadInt32(&u.NewData.FileObjectCount)),
			fmt.Sprintf("new-dirs:%v", atomic.LoadInt32(&u.NewData.DirObjectCount)),
			fmt.Sprintf("compression:%v", formatCompressionPercentage(atomic.LoadInt64(&u.NewData.OriginalContentBytes), atomic.LoadInt64(&u.NewData.PackedContentBytes))),
		)
	}

	return bits, col
}

func deltaBytes(b int64) string {
	if b > 0 {
		return "(+" + units.BytesString(b) + ")"
	}

	return ""
}
