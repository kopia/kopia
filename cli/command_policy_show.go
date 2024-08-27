package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

type commandPolicyShow struct {
	policyTargetFlags
	jo  jsonOutput
	out textOutput
}

func (c *commandPolicyShow) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("show", "Show snapshot policy.").Alias("get")
	c.policyTargetFlags.setup(cmd)
	c.jo.setup(svc, cmd)
	c.out.setup(svc)
	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandPolicyShow) run(ctx context.Context, rep repo.Repository) error {
	targets, err := c.policyTargets(ctx, rep)
	if err != nil {
		return err
	}

	for _, target := range targets {
		effective, definition, _, err := policy.GetEffectivePolicy(ctx, rep, target)
		if err != nil {
			return errors.Wrapf(err, "can't get effective policy for %q", target)
		}

		if c.jo.jsonOutput {
			c.out.printStdout("%s\n", c.jo.jsonBytes(effective))
		} else {
			printPolicy(&c.out, effective, definition)
		}
	}

	return nil
}

type policyTableRow struct {
	name  string
	value string
	def   string
}

func alignedPolicyTableRows(v []policyTableRow) string {
	var nameValueLen int

	const (
		nameValueSpace = "   "
		defSpace       = "   "
	)

	for _, it := range v {
		if it.value == "" {
			continue
		}

		t := it.name
		if it.value != "" {
			t += nameValueSpace + it.value
		}

		if len(t) > nameValueLen {
			nameValueLen = len(t)
		}
	}

	var lines []string

	for _, it := range v {
		l := it.name

		if it.value != "" || it.def != "" {
			if spaces := nameValueLen - len(l) - len(it.value); spaces > 0 {
				l += strings.Repeat(" ", spaces)
			}

			l += it.value
		}

		if it.def != "" {
			l += defSpace
			l += it.def
		}

		lines = append(lines, l)
	}

	return strings.Join(lines, "\n")
}

func definitionPointToString(target, src snapshot.SourceInfo) string {
	if src == target {
		return "(defined for this target)"
	}

	return "inherited from " + src.String()
}

func printPolicy(out *textOutput, p *policy.Policy, def *policy.Definition) {
	var rows []policyTableRow

	rows = appendRetentionPolicyRows(rows, p, def)
	rows = append(rows, policyTableRow{})
	rows = appendFilesPolicyValue(rows, p, def)
	rows = append(rows, policyTableRow{})
	rows = appendErrorHandlingPolicyRows(rows, p, def)
	rows = append(rows, policyTableRow{})
	rows = appendSchedulingPolicyRows(rows, p, def)
	rows = append(rows, policyTableRow{})
	rows = appendUploadPolicyRows(rows, p, def)
	rows = append(rows, policyTableRow{})
	rows = appendCompressionPolicyRows(rows, p, def)
	rows = append(rows, policyTableRow{})
	rows = appendMetadataCompressionPolicyRows(rows, p, def)
	rows = append(rows, policyTableRow{})
	rows = appendSplitterPolicyRows(rows, p, def)
	rows = append(rows, policyTableRow{})
	rows = appendActionsPolicyRows(rows, p, def)
	rows = append(rows, policyTableRow{})
	rows = appendOSSnapshotPolicyRows(rows, p, def)
	rows = append(rows, policyTableRow{})
	rows = appendLoggingPolicyRows(rows, p, def)

	out.printStdout("Policy for %v:\n\n%v\n", p.Target(), alignedPolicyTableRows(rows))
}

func appendRetentionPolicyRows(rows []policyTableRow, p *policy.Policy, def *policy.Definition) []policyTableRow {
	return append(rows,
		policyTableRow{"Retention:", "", ""},
		policyTableRow{"  Annual snapshots:", valueOrNotSet(p.RetentionPolicy.KeepAnnual), definitionPointToString(p.Target(), def.RetentionPolicy.KeepAnnual)},
		policyTableRow{"  Monthly snapshots:", valueOrNotSet(p.RetentionPolicy.KeepMonthly), definitionPointToString(p.Target(), def.RetentionPolicy.KeepMonthly)},
		policyTableRow{"  Weekly snapshots:", valueOrNotSet(p.RetentionPolicy.KeepWeekly), definitionPointToString(p.Target(), def.RetentionPolicy.KeepWeekly)},
		policyTableRow{"  Daily snapshots:", valueOrNotSet(p.RetentionPolicy.KeepDaily), definitionPointToString(p.Target(), def.RetentionPolicy.KeepDaily)},
		policyTableRow{"  Hourly snapshots:", valueOrNotSet(p.RetentionPolicy.KeepHourly), definitionPointToString(p.Target(), def.RetentionPolicy.KeepHourly)},
		policyTableRow{"  Latest snapshots:", valueOrNotSet(p.RetentionPolicy.KeepLatest), definitionPointToString(p.Target(), def.RetentionPolicy.KeepLatest)},
		policyTableRow{"  Ignore identical snapshots:", boolToString(p.RetentionPolicy.IgnoreIdenticalSnapshots.OrDefault(false)), definitionPointToString(p.Target(), def.RetentionPolicy.IgnoreIdenticalSnapshots)},
	)
}

func boolToString(v bool) string {
	if v {
		return "true"
	}

	return "false"
}

func logDetailToString(v policy.LogDetail) string {
	return fmt.Sprintf("%v", v)
}

func appendFilesPolicyValue(items []policyTableRow, p *policy.Policy, def *policy.Definition) []policyTableRow {
	items = append(items,
		policyTableRow{"Files policy:", "", ""},
		policyTableRow{
			"  Ignore cache directories:",
			boolToString(p.FilesPolicy.IgnoreCacheDirectories.OrDefault(true)),
			definitionPointToString(p.Target(), def.FilesPolicy.IgnoreCacheDirectories),
		})

	if len(p.FilesPolicy.IgnoreRules) > 0 {
		items = append(items, policyTableRow{
			"  Ignore rules:", "", definitionPointToString(p.Target(), def.FilesPolicy.IgnoreRules),
		})
		for _, rule := range p.FilesPolicy.IgnoreRules {
			items = append(items, policyTableRow{"    " + rule, "", ""})
		}
	} else {
		items = append(items, policyTableRow{"  No ignore rules:", "", ""})
	}

	if len(p.FilesPolicy.DotIgnoreFiles) > 0 {
		items = append(items, policyTableRow{
			"  Read ignore rules from files:", "",
			definitionPointToString(p.Target(), def.FilesPolicy.DotIgnoreFiles),
		})

		for _, dotFile := range p.FilesPolicy.DotIgnoreFiles {
			items = append(items, policyTableRow{"    " + dotFile, "", ""})
		}
	}

	if maxSize := p.FilesPolicy.MaxFileSize; maxSize > 0 {
		items = append(items, policyTableRow{
			"  Ignore files above:",
			units.BytesString(maxSize),
			definitionPointToString(p.Target(), def.FilesPolicy.MaxFileSize),
		})
	}

	items = append(items, policyTableRow{
		"  Scan one filesystem only:",
		boolToString(p.FilesPolicy.OneFileSystem.OrDefault(false)),
		definitionPointToString(p.Target(), def.FilesPolicy.OneFileSystem),
	})

	return items
}

func appendErrorHandlingPolicyRows(rows []policyTableRow, p *policy.Policy, def *policy.Definition) []policyTableRow {
	return append(rows,
		policyTableRow{"Error handling policy:", "", ""},
		policyTableRow{
			"  Ignore file read errors:",
			boolToString(p.ErrorHandlingPolicy.IgnoreFileErrors.OrDefault(false)),
			definitionPointToString(p.Target(), def.ErrorHandlingPolicy.IgnoreFileErrors),
		},
		policyTableRow{
			"  Ignore directory read errors:",
			boolToString(p.ErrorHandlingPolicy.IgnoreDirectoryErrors.OrDefault(false)),
			definitionPointToString(p.Target(), def.ErrorHandlingPolicy.IgnoreDirectoryErrors),
		},
		policyTableRow{
			"  Ignore unknown types:",
			boolToString(p.ErrorHandlingPolicy.IgnoreUnknownTypes.OrDefault(true)),
			definitionPointToString(p.Target(), def.ErrorHandlingPolicy.IgnoreUnknownTypes),
		},
	)
}

func appendLoggingPolicyRows(rows []policyTableRow, p *policy.Policy, def *policy.Definition) []policyTableRow {
	return append(rows,
		policyTableRow{
			fmt.Sprintf("Logging details (%v-none, %v-maximum):", policy.LogDetailNone, policy.LogDetailMax), "", "",
		},
		policyTableRow{
			"  Directory snapshotted:",
			logDetailToString(p.LoggingPolicy.Directories.Snapshotted.OrDefault(policy.LogDetailNone)),
			definitionPointToString(p.Target(), def.LoggingPolicy.Directories.Snapshotted),
		},
		policyTableRow{
			"  Directory ignored:",
			logDetailToString(p.LoggingPolicy.Directories.Ignored.OrDefault(policy.LogDetailNone)),
			definitionPointToString(p.Target(), def.LoggingPolicy.Directories.Ignored),
		},
		policyTableRow{
			"  Entry snapshotted:",
			logDetailToString(p.LoggingPolicy.Entries.Snapshotted.OrDefault(policy.LogDetailNone)),
			definitionPointToString(p.Target(), def.LoggingPolicy.Entries.Snapshotted),
		},
		policyTableRow{
			"  Entry ignored:",
			logDetailToString(p.LoggingPolicy.Entries.Ignored.OrDefault(policy.LogDetailNone)),
			definitionPointToString(p.Target(), def.LoggingPolicy.Entries.Ignored),
		},
		policyTableRow{
			"  Entry cache hit:",
			logDetailToString(p.LoggingPolicy.Entries.CacheHit.OrDefault(policy.LogDetailNone)),
			definitionPointToString(p.Target(), def.LoggingPolicy.Entries.CacheHit),
		},
		policyTableRow{
			"  Entry cache miss:",
			logDetailToString(p.LoggingPolicy.Entries.CacheMiss.OrDefault(policy.LogDetailNone)),
			definitionPointToString(p.Target(), def.LoggingPolicy.Entries.CacheMiss),
		},
	)
}

func appendUploadPolicyRows(rows []policyTableRow, p *policy.Policy, def *policy.Definition) []policyTableRow {
	return append(rows,
		policyTableRow{"Uploads:", "", ""},
		policyTableRow{"  Max parallel snapshots (server/UI):", valueOrNotSet(p.UploadPolicy.MaxParallelSnapshots), definitionPointToString(p.Target(), def.UploadPolicy.MaxParallelSnapshots)},
		policyTableRow{"  Max parallel file reads:", valueOrNotSet(p.UploadPolicy.MaxParallelFileReads), definitionPointToString(p.Target(), def.UploadPolicy.MaxParallelFileReads)},
		policyTableRow{"  Parallel upload above size:", valueOrNotSetOptionalInt64Bytes(p.UploadPolicy.ParallelUploadAboveSize), definitionPointToString(p.Target(), def.UploadPolicy.ParallelUploadAboveSize)},
	)
}

func appendSchedulingPolicyRows(rows []policyTableRow, p *policy.Policy, def *policy.Definition) []policyTableRow {
	rows = append(rows, policyTableRow{"Scheduling policy:", "", ""})

	hasAny := false

	rows = append(rows, policyTableRow{"  Scheduled snapshots:", "", ""})

	if p.SchedulingPolicy.Interval() != 0 {
		rows = append(rows, policyTableRow{
			"    Snapshot interval:",
			p.SchedulingPolicy.Interval().String(),
			definitionPointToString(p.Target(), def.SchedulingPolicy.IntervalSeconds),
		})

		hasAny = true
	}

	if len(p.SchedulingPolicy.TimesOfDay) > 0 {
		rows = append(rows,
			policyTableRow{
				"  Run missed snapshots:",
				boolToString(p.SchedulingPolicy.RunMissed.OrDefault(false)),
				definitionPointToString(p.Target(), def.SchedulingPolicy.RunMissed),
			},
			policyTableRow{
				"  Snapshot times:",
				"",
				definitionPointToString(p.Target(), def.SchedulingPolicy.TimesOfDay),
			})

		for _, tod := range p.SchedulingPolicy.TimesOfDay {
			rows = append(rows, policyTableRow{"    " + tod.String(), "", ""})
		}

		hasAny = true
	}

	if len(p.SchedulingPolicy.Cron) > 0 {
		rows = append(rows, policyTableRow{"  Crontab expressions:", "", definitionPointToString(p.Target(), def.SchedulingPolicy.Cron)})

		for _, cron := range p.SchedulingPolicy.Cron {
			rows = append(rows, policyTableRow{"    " + cron, "", ""})
		}

		hasAny = true
	}

	if !hasAny {
		rows = append(rows, policyTableRow{"    None.", "", ""})
	}

	rows = append(rows, policyTableRow{"  Manual snapshot:", boolToString(p.SchedulingPolicy.Manual), definitionPointToString(p.Target(), def.SchedulingPolicy.Manual)})

	return rows
}

func appendCompressionPolicyRows(rows []policyTableRow, p *policy.Policy, def *policy.Definition) []policyTableRow {
	if p.CompressionPolicy.CompressorName == "" || p.CompressionPolicy.CompressorName == "none" {
		rows = append(rows, policyTableRow{"Compression disabled.", "", ""})
		return rows
	}

	rows = append(rows,
		policyTableRow{"Compression:", "", ""},
		policyTableRow{"  Compressor:", string(p.CompressionPolicy.CompressorName), definitionPointToString(p.Target(), def.CompressionPolicy.CompressorName)})

	switch {
	case len(p.CompressionPolicy.OnlyCompress) > 0:
		rows = append(rows, policyTableRow{
			"  Only compress files with the following extensions:", "",
			definitionPointToString(p.Target(), def.CompressionPolicy.OnlyCompress),
		})

		for _, rule := range p.CompressionPolicy.OnlyCompress {
			rows = append(rows, policyTableRow{"     - " + rule, "", ""})
		}

	case len(p.CompressionPolicy.NeverCompress) > 0:
		rows = append(rows, policyTableRow{
			"  Compress all files except the following extensions:", "",
			definitionPointToString(p.Target(), def.CompressionPolicy.NeverCompress),
		})

		for _, rule := range p.CompressionPolicy.NeverCompress {
			rows = append(rows, policyTableRow{"    " + rule, "", ""})
		}

	default:
		rows = append(rows, policyTableRow{"  Compress files regardless of extensions.", "", ""})
	}

	switch {
	case p.CompressionPolicy.MaxSize > 0:
		rows = append(rows, policyTableRow{fmt.Sprintf(
			"  Only compress files between %v and %v.",
			units.BytesString(p.CompressionPolicy.MinSize),
			units.BytesString(p.CompressionPolicy.MaxSize)), "", ""})

	case p.CompressionPolicy.MinSize > 0:
		rows = append(rows, policyTableRow{fmt.Sprintf(
			"  Only compress files bigger than %v.",
			units.BytesString(p.CompressionPolicy.MinSize)), "", ""})

	default:
		rows = append(rows, policyTableRow{"  Compress files of all sizes.", "", ""})
	}

	return rows
}

func appendMetadataCompressionPolicyRows(rows []policyTableRow, p *policy.Policy, def *policy.Definition) []policyTableRow {
	if p.MetadataCompressionPolicy.CompressorName == "" || p.MetadataCompressionPolicy.CompressorName == "none" {
		rows = append(rows, policyTableRow{"Metadata compression disabled.", "", ""})
		return rows
	}

	return append(rows,
		policyTableRow{"Metadata compression:", "", ""},
		policyTableRow{"  Compressor:", string(p.MetadataCompressionPolicy.CompressorName), definitionPointToString(p.Target(), def.MetadataCompressionPolicy.CompressorName)})
}

func appendSplitterPolicyRows(rows []policyTableRow, p *policy.Policy, def *policy.Definition) []policyTableRow {
	algorithm := p.SplitterPolicy.Algorithm
	if algorithm == "" {
		algorithm = "(repository default)"
	}

	rows = append(rows,
		policyTableRow{"Splitter:", "", ""},
		policyTableRow{"  Algorithm override:", algorithm, definitionPointToString(p.Target(), def.SplitterPolicy.Algorithm)})

	return rows
}

func appendActionsPolicyRows(rows []policyTableRow, p *policy.Policy, def *policy.Definition) []policyTableRow {
	var anyActions bool

	if h := p.Actions.BeforeSnapshotRoot; h != nil {
		rows = append(rows,
			policyTableRow{"Run command before snapshot root:", "", definitionPointToString(p.Target(), def.Actions.BeforeSnapshotRoot)})
		rows = appendActionCommandRows(rows, h)

		anyActions = true
	}

	if h := p.Actions.AfterSnapshotRoot; h != nil {
		rows = append(rows, policyTableRow{"Run command after snapshot root:", "", definitionPointToString(p.Target(), def.Actions.AfterSnapshotRoot)})
		rows = appendActionCommandRows(rows, h)

		anyActions = true
	}

	if h := p.Actions.BeforeFolder; h != nil {
		rows = append(rows, policyTableRow{"Run command before this folder:", "", "(non-inheritable)"})
		rows = appendActionCommandRows(rows, h)

		anyActions = true
	}

	if h := p.Actions.AfterFolder; h != nil {
		rows = append(rows, policyTableRow{"Run command after this folder:", "", "(non-inheritable)"})
		rows = appendActionCommandRows(rows, h)

		anyActions = true
	}

	if !anyActions {
		rows = append(rows, policyTableRow{"No actions defined.", "", ""})
	}

	return rows
}

func appendActionCommandRows(rows []policyTableRow, h *policy.ActionCommand) []policyTableRow {
	if h.Script != "" {
		rows = append(rows,
			policyTableRow{"  Embedded script (stored in repository):", "", ""},
			policyTableRow{indentMultilineString(h.Script, "    "), "", ""},
		)
	} else {
		rows = append(rows,
			policyTableRow{"  Command:", "", ""},
			policyTableRow{"    " + h.Command + " " + strings.Join(h.Arguments, " "), "", ""})
	}

	actualMode := h.Mode
	if actualMode == "" {
		actualMode = "sync"
	}

	rows = append(rows,
		policyTableRow{"  Mode:", actualMode, ""},
		policyTableRow{"  Timeout:", (time.Second * time.Duration(h.TimeoutSeconds)).String(), ""},
		policyTableRow{"", "", ""},
	)

	return rows
}

func appendOSSnapshotPolicyRows(rows []policyTableRow, p *policy.Policy, def *policy.Definition) []policyTableRow {
	rows = append(rows,
		policyTableRow{"OS-level snapshot support:", "", ""},
		policyTableRow{"  Volume Shadow Copy:", p.OSSnapshotPolicy.VolumeShadowCopy.Enable.String(), definitionPointToString(p.Target(), def.OSSnapshotPolicy.VolumeShadowCopy.Enable)},
	)

	return rows
}

func valueOrNotSet(p *policy.OptionalInt) string {
	if p == nil {
		return "-"
	}

	return fmt.Sprintf("%v", *p)
}

func valueOrNotSetOptionalInt64Bytes(p *policy.OptionalInt64) string {
	if p == nil {
		return "-"
	}

	return units.BytesString(*p)
}
