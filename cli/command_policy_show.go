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
		effective, policies, err := policy.GetEffectivePolicy(ctx, rep, target)
		if err != nil {
			return errors.Wrapf(err, "can't get effective policy for %q", target)
		}

		if c.jo.jsonOutput {
			c.out.printStdout("%s\n", c.jo.jsonBytes(effective))
		} else {
			printPolicy(&c.out, effective, policies)
		}
	}

	return nil
}

func getDefinitionPoint(target snapshot.SourceInfo, parents []*policy.Policy, match func(p *policy.Policy) bool) string {
	for _, p := range parents {
		if match(p) {
			if p.Target() == target {
				return "(defined for this target)"
			}

			return "inherited from " + p.Target().String()
		}

		if p.NoParent {
			break
		}
	}

	return "(default)"
}

func containsString(s []string, v string) bool {
	for _, item := range s {
		if item == v {
			return true
		}
	}

	return false
}

func printPolicy(out *textOutput, p *policy.Policy, parents []*policy.Policy) {
	out.printStdout("Policy for %v:\n\n", p.Target())

	printRetentionPolicy(out, p, parents)
	out.printStdout("\n")
	printFilesPolicy(out, p, parents)
	out.printStdout("\n")
	printErrorHandlingPolicy(out, p, parents)
	out.printStdout("\n")
	printSchedulingPolicy(out, p, parents)
	out.printStdout("\n")
	printCompressionPolicy(out, p, parents)
	out.printStdout("\n")
	printActions(out, p, parents)
}

func printRetentionPolicy(out *textOutput, p *policy.Policy, parents []*policy.Policy) {
	out.printStdout("Retention:\n")
	out.printStdout("  Annual snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepAnnual),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.RetentionPolicy.KeepAnnual != nil
		}))
	out.printStdout("  Monthly snapshots: %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepMonthly),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.RetentionPolicy.KeepMonthly != nil
		}))
	out.printStdout("  Weekly snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepWeekly),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.RetentionPolicy.KeepWeekly != nil
		}))
	out.printStdout("  Daily snapshots:   %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepDaily),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.RetentionPolicy.KeepDaily != nil
		}))
	out.printStdout("  Hourly snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepHourly),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.RetentionPolicy.KeepHourly != nil
		}))
	out.printStdout("  Latest snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepLatest),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.RetentionPolicy.KeepLatest != nil
		}))
}

func printFilesPolicy(out *textOutput, p *policy.Policy, parents []*policy.Policy) {
	out.printStdout("Files policy:\n")

	out.printStdout("  Ignore cache directories:       %5v       %v\n",
		p.FilesPolicy.IgnoreCacheDirectoriesOrDefault(true),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.FilesPolicy.IgnoreCacheDirs != nil
		}))

	if len(p.FilesPolicy.IgnoreRules) > 0 {
		out.printStdout("  Ignore rules:\n")
	} else {
		out.printStdout("  No ignore rules.\n")
	}

	for _, rule := range p.FilesPolicy.IgnoreRules {
		rule := rule
		out.printStdout("    %-30v %v\n", rule, getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return containsString(pol.FilesPolicy.IgnoreRules, rule)
		}))
	}

	if len(p.FilesPolicy.DotIgnoreFiles) > 0 {
		out.printStdout("  Read ignore rules from files:\n")
	}

	for _, dotFile := range p.FilesPolicy.DotIgnoreFiles {
		dotFile := dotFile
		out.printStdout("    %-30v %v\n", dotFile, getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return containsString(pol.FilesPolicy.DotIgnoreFiles, dotFile)
		}))
	}

	if maxSize := p.FilesPolicy.MaxFileSize; maxSize > 0 {
		out.printStdout("  Ignore files above: %10v  %v\n",
			units.BytesStringBase2(maxSize),
			getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
				return pol.FilesPolicy.MaxFileSize != 0
			}))
	}

	out.printStdout("  Scan one filesystem only:       %5v       %v\n",
		p.FilesPolicy.OneFileSystemOrDefault(false),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.FilesPolicy.OneFileSystem != nil
		}))
}

func printErrorHandlingPolicy(out *textOutput, p *policy.Policy, parents []*policy.Policy) {
	out.printStdout("Error handling policy:\n")

	out.printStdout("  Ignore file read errors:       %5v       %v\n",
		p.ErrorHandlingPolicy.IgnoreFileErrorsOrDefault(false),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.ErrorHandlingPolicy.IgnoreFileErrors != nil
		}))

	out.printStdout("  Ignore directory read errors:  %5v       %v\n",
		p.ErrorHandlingPolicy.IgnoreDirectoryErrorsOrDefault(false),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.ErrorHandlingPolicy.IgnoreDirectoryErrors != nil
		}))

	out.printStdout("  Ignore unknown types:          %5v       %v\n",
		p.ErrorHandlingPolicy.IgnoreUnknownTypesOrDefault(true),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.ErrorHandlingPolicy.IgnoreUnknownTypes != nil
		}))
}

func printSchedulingPolicy(out *textOutput, p *policy.Policy, parents []*policy.Policy) {
	out.printStdout("Scheduling policy:\n")

	any := false

	out.printStdout("  Scheduled snapshots:\n")

	if p.SchedulingPolicy.Interval() != 0 {
		out.printStdout("    Snapshot interval:   %10v  %v\n", p.SchedulingPolicy.Interval(), getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.SchedulingPolicy.Interval() != 0
		}))

		any = true
	}

	if len(p.SchedulingPolicy.TimesOfDay) > 0 {
		out.printStdout("    Snapshot times:\n")

		for _, tod := range p.SchedulingPolicy.TimesOfDay {
			tod := tod
			out.printStdout("      %9v                      %v\n", tod, getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
				for _, t := range pol.SchedulingPolicy.TimesOfDay {
					if t == tod {
						return true
					}
				}

				return false
			}))
		}

		any = true
	}

	if !any {
		out.printStdout("    None\n")
	}

	out.printStdout("  Manual snapshot:           %5v   %v\n",
		p.SchedulingPolicy.Manual,
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.SchedulingPolicy.Manual
		}))
}

func printCompressionPolicy(out *textOutput, p *policy.Policy, parents []*policy.Policy) {
	if p.CompressionPolicy.CompressorName != "" && p.CompressionPolicy.CompressorName != "none" {
		out.printStdout("Compression:\n")
		out.printStdout("  Compressor: %q %v\n", p.CompressionPolicy.CompressorName, getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.CompressionPolicy.CompressorName != ""
		}))
	} else {
		out.printStdout("Compression disabled.\n")
		return
	}

	switch {
	case len(p.CompressionPolicy.OnlyCompress) > 0:
		out.printStdout("  Only compress files with the following extensions:\n")

		for _, rule := range p.CompressionPolicy.OnlyCompress {
			rule := rule
			out.printStdout("    %-30v %v\n", rule, getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
				return containsString(pol.CompressionPolicy.OnlyCompress, rule)
			}))
		}

	case len(p.CompressionPolicy.NeverCompress) > 0:
		out.printStdout("  Compress all files except the following extensions:\n")

		for _, rule := range p.CompressionPolicy.NeverCompress {
			rule := rule
			out.printStdout("    %-30v %v\n", rule, getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
				return containsString(pol.CompressionPolicy.NeverCompress, rule)
			}))
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

func printActions(out *textOutput, p *policy.Policy, parents []*policy.Policy) {
	var anyActions bool

	if h := p.Actions.BeforeSnapshotRoot; h != nil {
		out.printStdout("Run command before snapshot root:  %v\n", getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.Actions.BeforeSnapshotRoot == h
		}))

		printActionCommand(out, h)

		anyActions = true
	}

	if h := p.Actions.AfterSnapshotRoot; h != nil {
		out.printStdout("Run command after snapshot root:   %v\n", getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.Actions.AfterSnapshotRoot == h
		}))
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
