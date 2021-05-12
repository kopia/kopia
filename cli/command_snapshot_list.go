package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
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
	cmd.Flag("all", "Show all shapshots (not just current username/host)").Short('a').BoolVar(&c.snapshotListShowAll)
	cmd.Flag("max-results", "Maximum number of entries per source.").Short('n').IntVar(&c.maxResultsPerPath)
	cmd.Flag("tags", "Tag filters to apply on the list items. Must be provided in the <key>:<value> format.").StringsVar(&c.snapshotListTags)
	c.jo.setup(svc, cmd)
	c.out.setup(svc)
	cmd.Action(svc.repositoryReaderAction(c.run))
}

func findSnapshotsForSource(ctx context.Context, rep repo.Repository, sourceInfo snapshot.SourceInfo, tags map[string]string) (manifestIDs []manifest.ID, relPath string, err error) {
	for len(sourceInfo.Path) > 0 {
		list, err := snapshot.ListSnapshotManifests(ctx, rep, &sourceInfo, tags)
		if err != nil {
			return nil, "", errors.Wrapf(err, "error listing manifests for %v", sourceInfo)
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

func findManifestIDs(ctx context.Context, rep repo.Repository, source string, tags map[string]string) ([]manifest.ID, string, error) {
	if source == "" {
		man, err := snapshot.ListSnapshotManifests(ctx, rep, nil, tags)
		return man, "", errors.Wrap(err, "error listing all snapshot manifests")
	}

	si, err := snapshot.ParseSourceInfo(source, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
	if err != nil {
		return nil, "", errors.Errorf("invalid directory: '%s': %s", source, err)
	}

	manifestIDs, relPath, err := findSnapshotsForSource(ctx, rep, si, tags)
	if relPath != "" {
		relPath = "/" + relPath
	}

	return manifestIDs, relPath, err
}

func (c *commandSnapshotList) run(ctx context.Context, rep repo.Repository) error {
	var jl jsonList

	jl.begin(&c.jo)
	defer jl.end()

	tags, err := getTags(c.snapshotListTags)
	if err != nil {
		return err
	}

	manifestIDs, relPath, err := findManifestIDs(ctx, rep, c.snapshotListPath, tags)
	if err != nil {
		return err
	}

	manifests, err := snapshot.LoadSnapshots(ctx, rep, manifestIDs)
	if err != nil {
		return errors.Wrap(err, "unable to load snapshots")
	}

	if c.jo.jsonOutput {
		for _, snapshotGroup := range snapshot.GroupBySource(manifests) {
			for _, m := range snapshotGroup {
				jl.emit(m)
			}
		}

		return nil
	}

	return c.outputManifestGroups(ctx, rep, manifests, strings.Split(relPath, "/"))
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

func (c *commandSnapshotList) outputManifestGroups(ctx context.Context, rep repo.Repository, manifests []*snapshot.Manifest, relPathParts []string) error {
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

		pol, _, err := policy.GetEffectivePolicy(ctx, rep, src)
		if err != nil {
			log(ctx).Errorf("unable to determine effective policy for %v", src)
		} else {
			pol.RetentionPolicy.ComputeRetentionReasons(snapshotGroup)
		}

		if err := c.outputManifestFromSingleSource(ctx, rep, snapshotGroup, relPathParts); err != nil {
			return err
		}
	}

	if !anyOutput && !c.snapshotListShowAll && len(manifests) > 0 {
		log(ctx).Infof("No snapshots found. Pass --all to show snapshots from all users/hosts.\n")
	}

	return nil
}

func (c *commandSnapshotList) outputManifestFromSingleSource(ctx context.Context, rep repo.Repository, manifests []*snapshot.Manifest, parts []string) error {
	var (
		count             int
		lastTotalFileSize int64
		previousOID       object.ID
		elidedCount       int
		maxElidedTime     time.Time
	)

	manifests = snapshot.SortByTime(manifests, false)
	if c.maxResultsPerPath > 0 && len(manifests) > c.maxResultsPerPath {
		manifests = manifests[len(manifests)-c.maxResultsPerPath:]
	}

	outputElided := func() {
		if elidedCount > 0 {
			c.out.printStdout(
				"  + %v identical snapshots until %v\n",
				elidedCount,
				formatTimestamp(maxElidedTime),
			)
		}
	}

	for _, m := range manifests {
		root, err := snapshotfs.SnapshotRoot(rep, m)
		if err != nil {
			c.out.printStdout("  %v <ERROR> %v\n", formatTimestamp(m.StartTime), err)
			continue
		}

		ent, err := snapshotfs.GetNestedEntry(ctx, root, parts)
		if err != nil {
			c.out.printStdout("  %v <ERROR> %v\n", formatTimestamp(m.StartTime), err)
			continue
		}

		if _, ok := ent.(object.HasObjectID); !ok {
			log(ctx).Errorf("entry does not have object ID: %v", ent, err)
			continue
		}

		if m.IncompleteReason != "" && !c.snapshotListIncludeIncomplete {
			continue
		}

		bits, col := c.entryBits(ctx, m, ent, lastTotalFileSize)

		oid := ent.(object.HasObjectID).ObjectID()
		if !c.snapshotListShowIdentical && oid == previousOID {
			elidedCount++

			maxElidedTime = m.StartTime

			continue
		}

		outputElided()

		elidedCount = 0
		previousOID = oid

		col.Fprint(c.out.stdout(), fmt.Sprintf("  %v %v %v\n", formatTimestamp(m.StartTime), oid, strings.Join(bits, " "))) //nolint:errcheck

		count++

		if m.IncompleteReason == "" {
			lastTotalFileSize = m.Stats.TotalFileSize
		}
	}

	outputElided()

	return nil
}

func (c *commandSnapshotList) entryBits(ctx context.Context, m *snapshot.Manifest, ent fs.Entry, lastTotalFileSize int64) (bits []string, col *color.Color) {
	col = color.New() // default color

	if m.IncompleteReason != "" {
		bits = append(bits, "incomplete:"+m.IncompleteReason)
	}

	bits = append(bits,
		maybeHumanReadableBytes(c.snapshotListShowHumanReadable, ent.Size()),
		fmt.Sprintf("%v", ent.Mode()))
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

	if dws, ok := ent.(fs.DirectoryWithSummary); ok {
		if s, _ := dws.Summary(ctx); s != nil {
			bits = append(bits,
				fmt.Sprintf("files:%v", s.TotalFileCount),
				fmt.Sprintf("dirs:%v", s.TotalDirCount))
			if s.FatalErrorCount > 0 {
				bits = append(bits, fmt.Sprintf("errors:%v", s.FatalErrorCount))
				col = errorColor
			}
		}
	}

	if c.snapshotListShowRetentionReasons {
		if len(m.RetentionReasons) > 0 {
			bits = append(bits, "("+strings.Join(m.RetentionReasons, ",")+")")
		}
	}

	return bits, col
}

func deltaBytes(b int64) string {
	if b > 0 {
		return "(+" + units.BytesStringBase10(b) + ")"
	}

	return ""
}
