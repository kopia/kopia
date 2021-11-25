package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

type commandPolicyShow struct {
	global  bool
	targets []string
	jo      jsonOutput
	out     textOutput
}

func (c *commandPolicyShow) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("show", "Show snapshot policy.").Alias("get")
	cmd.Flag("global", "Get global policy").BoolVar(&c.global)
	cmd.Arg("target", "Target to show the policy for").StringsVar(&c.targets)
	c.jo.setup(svc, cmd)
	c.out.setup(svc)
	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandPolicyShow) run(ctx context.Context, rep repo.Repository) error {
	targets, err := policyTargets(ctx, rep, c.global, c.targets)
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

func definitionPointToString(target, src snapshot.SourceInfo) string {
	if src == target {
		return "(defined for this target)"
	}

	return "inherited from " + src.String()
}

func printPolicy(out *textOutput, p *policy.Policy, def *policy.Definition) {
	out.printStdout("Policy for %v:\n\n", p.Target())

	printRetentionPolicy(out, p, def)
	out.printStdout("\n")
	printFilesPolicy(out, p, def)
	out.printStdout("\n")
	printErrorHandlingPolicy(out, p, def)
	out.printStdout("\n")
	printSchedulingPolicy(out, p, def)
	out.printStdout("\n")
	printCompressionPolicy(out, p, def)
	out.printStdout("\n")
	printActions(out, p, def)
	out.printStdout("\n")
	printLoggingPolicy(out, p, def)
}

func printRetentionPolicy(out *textOutput, p *policy.Policy, def *policy.Definition) {
	out.printStdout("Retention:\n")
	out.printStdout("  Annual snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepAnnual),
		definitionPointToString(p.Target(), def.RetentionPolicy.KeepAnnual))
	out.printStdout("  Monthly snapshots: %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepMonthly),
		definitionPointToString(p.Target(), def.RetentionPolicy.KeepMonthly))
	out.printStdout("  Weekly snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepWeekly),
		definitionPointToString(p.Target(), def.RetentionPolicy.KeepWeekly))
	out.printStdout("  Daily snapshots:   %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepDaily),
		definitionPointToString(p.Target(), def.RetentionPolicy.KeepDaily))
	out.printStdout("  Hourly snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepHourly),
		definitionPointToString(p.Target(), def.RetentionPolicy.KeepHourly))
	out.printStdout("  Latest snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepLatest),
		definitionPointToString(p.Target(), def.RetentionPolicy.KeepLatest))
}

func printFilesPolicy(out *textOutput, p *policy.Policy, def *policy.Definition) {
	out.printStdout("Files policy:\n")

	out.printStdout("  Ignore cache directories:       %5v       %v\n",
		p.FilesPolicy.IgnoreCacheDirectories.OrDefault(true),
		definitionPointToString(p.Target(), def.FilesPolicy.IgnoreCacheDirectories))

	if len(p.FilesPolicy.IgnoreRules) > 0 {
		out.printStdout("  Ignore rules:                               %v\n",
			definitionPointToString(p.Target(), def.FilesPolicy.IgnoreRules))
	} else {
		out.printStdout("  No ignore rules.\n")
	}

	for _, rule := range p.FilesPolicy.IgnoreRules {
		out.printStdout("    %-30v\n", rule)
	}

	if len(p.FilesPolicy.DotIgnoreFiles) > 0 {
		out.printStdout("  Read ignore rules from files:               %v\n",
			definitionPointToString(p.Target(), def.FilesPolicy.DotIgnoreFiles))
	}

	for _, dotFile := range p.FilesPolicy.DotIgnoreFiles {
		out.printStdout("    %-30v\n", dotFile)
	}

	if maxSize := p.FilesPolicy.MaxFileSize; maxSize > 0 {
		out.printStdout("  Ignore files above: %10v  %v\n",
			units.BytesStringBase2(maxSize),
			definitionPointToString(p.Target(), def.FilesPolicy.MaxFileSize))
	}

	out.printStdout("  Scan one filesystem only:       %5v       %v\n",
		p.FilesPolicy.OneFileSystem.OrDefault(false),
		definitionPointToString(p.Target(), def.FilesPolicy.OneFileSystem))
}

func printErrorHandlingPolicy(out *textOutput, p *policy.Policy, def *policy.Definition) {
	out.printStdout("Error handling policy:\n")

	out.printStdout("  Ignore file read errors:       %5v       %v\n",
		p.ErrorHandlingPolicy.IgnoreFileErrors.OrDefault(false),
		definitionPointToString(p.Target(), def.ErrorHandlingPolicy.IgnoreFileErrors))

	out.printStdout("  Ignore directory read errors:  %5v       %v\n",
		p.ErrorHandlingPolicy.IgnoreDirectoryErrors.OrDefault(false),
		definitionPointToString(p.Target(), def.ErrorHandlingPolicy.IgnoreDirectoryErrors))

	out.printStdout("  Ignore unknown types:          %5v       %v\n",
		p.ErrorHandlingPolicy.IgnoreUnknownTypes.OrDefault(true),
		definitionPointToString(p.Target(), def.ErrorHandlingPolicy.IgnoreUnknownTypes))
}

func printLoggingPolicy(out *textOutput, p *policy.Policy, def *policy.Definition) {
	out.printStdout("Logging details (%v-none, %v-maximum):\n", policy.LogDetailNone, policy.LogDetailMax)

	out.printStdout("  Directory snapshotted:  %5v       %v\n",
		p.LoggingPolicy.Directories.Snapshotted.OrDefault(policy.LogDetailNone),
		definitionPointToString(p.Target(), def.LoggingPolicy.Directories.Snapshotted))

	out.printStdout("  Directory ignored:      %5v       %v\n",
		p.LoggingPolicy.Directories.Ignored.OrDefault(policy.LogDetailNone),
		definitionPointToString(p.Target(), def.LoggingPolicy.Directories.Ignored))

	out.printStdout("  Entry snapshotted:      %5v       %v\n",
		p.LoggingPolicy.Entries.Snapshotted.OrDefault(policy.LogDetailNone),
		definitionPointToString(p.Target(), def.LoggingPolicy.Entries.Snapshotted))

	out.printStdout("  Entry ignored:          %5v       %v\n",
		p.LoggingPolicy.Entries.Ignored.OrDefault(policy.LogDetailNone),
		definitionPointToString(p.Target(), def.LoggingPolicy.Entries.Ignored))

	out.printStdout("  Entry cache hit         %5v       %v\n",
		p.LoggingPolicy.Entries.CacheHit.OrDefault(policy.LogDetailNone),
		definitionPointToString(p.Target(), def.LoggingPolicy.Entries.CacheHit))

	out.printStdout("  Entry cache miss        %5v       %v\n",
		p.LoggingPolicy.Entries.CacheMiss.OrDefault(policy.LogDetailNone),
		definitionPointToString(p.Target(), def.LoggingPolicy.Entries.CacheMiss))
}

func printSchedulingPolicy(out *textOutput, p *policy.Policy, def *policy.Definition) {
	out.printStdout("Scheduling policy:\n")

	any := false

	out.printStdout("  Scheduled snapshots:\n")

	if p.SchedulingPolicy.Interval() != 0 {
		out.printStdout("    Snapshot interval:   %10v  %v\n", p.SchedulingPolicy.Interval(),
			definitionPointToString(p.Target(), def.SchedulingPolicy.IntervalSeconds))

		any = true
	}

	if len(p.SchedulingPolicy.TimesOfDay) > 0 {
		out.printStdout("    Snapshot times: %v\n", definitionPointToString(p.Target(), def.SchedulingPolicy.TimesOfDay))

		for _, tod := range p.SchedulingPolicy.TimesOfDay {
			out.printStdout("      %9v\n", tod)
		}

		any = true
	}

	if !any {
		out.printStdout("    None\n")
	}

	out.printStdout("  Manual snapshot:           %5v   %v\n",
		p.SchedulingPolicy.Manual,
		definitionPointToString(p.Target(), def.SchedulingPolicy.Manual))
}

func printCompressionPolicy(out *textOutput, p *policy.Policy, def *policy.Definition) {
	if p.CompressionPolicy.CompressorName != "" && p.CompressionPolicy.CompressorName != "none" {
		out.printStdout("Compression:\n")
		out.printStdout("  Compressor: %q %v\n", p.CompressionPolicy.CompressorName,
			definitionPointToString(p.Target(), def.CompressionPolicy.CompressorName))
	} else {
		out.printStdout("Compression disabled.\n")
		return
	}

	switch {
	case len(p.CompressionPolicy.OnlyCompress) > 0:
		out.printStdout("  Only compress files with the following extensions: %v\n",
			definitionPointToString(p.Target(), def.CompressionPolicy.OnlyCompress))

		for _, rule := range p.CompressionPolicy.OnlyCompress {
			out.printStdout("    %-30v\n", rule)
		}

	case len(p.CompressionPolicy.NeverCompress) > 0:
		out.printStdout("  Compress all files except the following extensions: %v\n",
			definitionPointToString(p.Target(), def.CompressionPolicy.NeverCompress))

		for _, rule := range p.CompressionPolicy.NeverCompress {
			out.printStdout("    %-30v\n", rule)
		}

	default:
		out.printStdout("  Compress files regardless of extensions.\n")
	}

	switch {
	case p.CompressionPolicy.MaxSize > 0:
		out.printStdout("  Only compress files between %v and %v.\n", units.BytesStringBase10(p.CompressionPolicy.MinSize), units.BytesStringBase10(p.CompressionPolicy.MaxSize))

	case p.CompressionPolicy.MinSize > 0:
		out.printStdout("  Only compress files bigger than %v.\n", units.BytesStringBase10(p.CompressionPolicy.MinSize))

	default:
		out.printStdout("  Compress files of all sizes.\n")
	}
}

func printActions(out *textOutput, p *policy.Policy, def *policy.Definition) {
	var anyActions bool

	if h := p.Actions.BeforeSnapshotRoot; h != nil {
		out.printStdout("Run command before snapshot root:  %v\n",
			definitionPointToString(p.Target(), def.Actions.BeforeSnapshotRoot))

		printActionCommand(out, h)

		anyActions = true
	}

	if h := p.Actions.AfterSnapshotRoot; h != nil {
		out.printStdout("Run command after snapshot root:   %v\n",
			definitionPointToString(p.Target(), def.Actions.AfterSnapshotRoot))
		printActionCommand(out, h)

		anyActions = true
	}

	if h := p.Actions.BeforeFolder; h != nil {
		out.printStdout("Run command before this folder:    (non-inheritable)\n")

		printActionCommand(out, h)

		anyActions = true
	}

	if h := p.Actions.AfterFolder; h != nil {
		out.printStdout("Run command after this folder:    (non-inheritable)\n")
		printActionCommand(out, h)

		anyActions = true
	}

	if !anyActions {
		out.printStdout("No actions defined.\n")
	}
}

func printActionCommand(out *textOutput, h *policy.ActionCommand) {
	if h.Script != "" {
		out.printStdout("  Embedded Script: %q\n", h.Script)
	} else {
		out.printStdout("  Command: %v %v\n", h.Command, strings.Join(h.Arguments, " "))
	}

	out.printStdout("  Mode: %v\n", h.Mode)
	out.printStdout("  Timeout: %v\n", h.TimeoutSeconds)
	out.printStdout("\n")
}

func valueOrNotSet(p *int) string {
	if p == nil {
		return "-"
	}

	return fmt.Sprintf("%v", *p)
}
