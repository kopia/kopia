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

var (
	policyShowCommand = policyCommands.Command("show", "Show snapshot policy.").Alias("get")
	policyShowGlobal  = policyShowCommand.Flag("global", "Get global policy").Bool()
	policyShowTargets = policyShowCommand.Arg("target", "Target to show the policy for").Strings()
	policyShowJSON    = policyShowCommand.Flag("json", "Show JSON").Short('j').Bool()
)

func init() {
	policyShowCommand.Action(repositoryReaderAction(showPolicy))
}

func showPolicy(ctx context.Context, rep repo.Repository) error {
	targets, err := policyTargets(ctx, rep, policyShowGlobal, policyShowTargets)
	if err != nil {
		return err
	}

	for _, target := range targets {
		effective, policies, err := policy.GetEffectivePolicy(ctx, rep, target)
		if err != nil {
			return errors.Wrapf(err, "can't get effective policy for %q", target)
		}

		if *policyShowJSON {
			fmt.Println(effective)
		} else {
			printPolicy(effective, policies)
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

func printPolicy(p *policy.Policy, parents []*policy.Policy) {
	printStdout("Policy for %v:\n\n", p.Target())

	printRetentionPolicy(p, parents)
	printStdout("\n")
	printFilesPolicy(p, parents)
	printStdout("\n")
	printErrorHandlingPolicy(p, parents)
	printStdout("\n")
	printSchedulingPolicy(p, parents)
	printStdout("\n")
	printCompressionPolicy(p, parents)
	printStdout("\n")
	printActions(p, parents)
}

func printRetentionPolicy(p *policy.Policy, parents []*policy.Policy) {
	printStdout("Retention:\n")
	printStdout("  Annual snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepAnnual),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.RetentionPolicy.KeepAnnual != nil
		}))
	printStdout("  Monthly snapshots: %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepMonthly),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.RetentionPolicy.KeepMonthly != nil
		}))
	printStdout("  Weekly snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepWeekly),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.RetentionPolicy.KeepWeekly != nil
		}))
	printStdout("  Daily snapshots:   %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepDaily),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.RetentionPolicy.KeepDaily != nil
		}))
	printStdout("  Hourly snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepHourly),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.RetentionPolicy.KeepHourly != nil
		}))
	printStdout("  Latest snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepLatest),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.RetentionPolicy.KeepLatest != nil
		}))
}

func printFilesPolicy(p *policy.Policy, parents []*policy.Policy) {
	printStdout("Files policy:\n")

	printStdout("  Ignore cache directories:       %5v       %v\n",
		p.FilesPolicy.IgnoreCacheDirectoriesOrDefault(true),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.FilesPolicy.IgnoreCacheDirs != nil
		}))

	if len(p.FilesPolicy.IgnoreRules) > 0 {
		printStdout("  Ignore rules:\n")
	} else {
		printStdout("  No ignore rules.\n")
	}

	for _, rule := range p.FilesPolicy.IgnoreRules {
		rule := rule
		printStdout("    %-30v %v\n", rule, getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return containsString(pol.FilesPolicy.IgnoreRules, rule)
		}))
	}

	if len(p.FilesPolicy.DotIgnoreFiles) > 0 {
		printStdout("  Read ignore rules from files:\n")
	}

	for _, dotFile := range p.FilesPolicy.DotIgnoreFiles {
		dotFile := dotFile
		printStdout("    %-30v %v\n", dotFile, getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return containsString(pol.FilesPolicy.DotIgnoreFiles, dotFile)
		}))
	}

	if maxSize := p.FilesPolicy.MaxFileSize; maxSize > 0 {
		printStdout("  Ignore files above: %10v  %v\n",
			units.BytesStringBase2(maxSize),
			getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
				return pol.FilesPolicy.MaxFileSize != 0
			}))
	}

	printStdout("  Scan one filesystem only:       %5v       %v\n",
		p.FilesPolicy.OneFileSystemOrDefault(false),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.FilesPolicy.OneFileSystem != nil
		}))
}

func printErrorHandlingPolicy(p *policy.Policy, parents []*policy.Policy) {
	printStdout("Error handling policy:\n")

	printStdout("  Ignore file read errors:       %5v       %v\n",
		p.ErrorHandlingPolicy.IgnoreFileErrorsOrDefault(false),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.ErrorHandlingPolicy.IgnoreFileErrors != nil
		}))

	printStdout("  Ignore directory read errors:  %5v       %v\n",
		p.ErrorHandlingPolicy.IgnoreDirectoryErrorsOrDefault(false),
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.ErrorHandlingPolicy.IgnoreDirectoryErrors != nil
		}))
}

func printSchedulingPolicy(p *policy.Policy, parents []*policy.Policy) {
	printStdout("Scheduling policy:\n")

	any := false

	printStdout("  Scheduled snapshots:\n")

	if p.SchedulingPolicy.Interval() != 0 {
		printStdout("    Snapshot interval:   %10v  %v\n", p.SchedulingPolicy.Interval(), getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.SchedulingPolicy.Interval() != 0
		}))

		any = true
	}

	if len(p.SchedulingPolicy.TimesOfDay) > 0 {
		printStdout("    Snapshot times:\n")

		for _, tod := range p.SchedulingPolicy.TimesOfDay {
			tod := tod
			printStdout("      %9v                      %v\n", tod, getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
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
		printStdout("    None\n")
	}

	printStdout("  Manual snapshot:           %5v   %v\n",
		p.SchedulingPolicy.Manual,
		getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.SchedulingPolicy.Manual
		}))
}

func printCompressionPolicy(p *policy.Policy, parents []*policy.Policy) {
	if p.CompressionPolicy.CompressorName != "" && p.CompressionPolicy.CompressorName != "none" {
		printStdout("Compression:\n")
		printStdout("  Compressor: %q %v\n", p.CompressionPolicy.CompressorName, getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.CompressionPolicy.CompressorName != ""
		}))
	} else {
		printStdout("Compression disabled.\n")
		return
	}

	switch {
	case len(p.CompressionPolicy.OnlyCompress) > 0:
		printStdout("  Only compress files with the following extensions:\n")

		for _, rule := range p.CompressionPolicy.OnlyCompress {
			rule := rule
			printStdout("    %-30v %v\n", rule, getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
				return containsString(pol.CompressionPolicy.OnlyCompress, rule)
			}))
		}

	case len(p.CompressionPolicy.NeverCompress) > 0:
		printStdout("  Compress all files except the following extensions:\n")

		for _, rule := range p.CompressionPolicy.NeverCompress {
			rule := rule
			printStdout("    %-30v %v\n", rule, getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
				return containsString(pol.CompressionPolicy.NeverCompress, rule)
			}))
		}

	default:
		printStdout("  Compress files regardless of extensions.\n")
	}

	switch {
	case p.CompressionPolicy.MaxSize > 0:
		printStdout("  Only compress files between %v and %v.\n", units.BytesStringBase10(p.CompressionPolicy.MinSize), units.BytesStringBase10(p.CompressionPolicy.MaxSize))

	case p.CompressionPolicy.MinSize > 0:
		printStdout("  Only compress files bigger than %v.\n", units.BytesStringBase10(p.CompressionPolicy.MinSize))

	default:
		printStdout("  Compress files of all sizes.\n")
	}
}

func printActions(p *policy.Policy, parents []*policy.Policy) {
	var anyActions bool

	if h := p.Actions.BeforeSnapshotRoot; h != nil {
		printStdout("Run command before snapshot root:  %v\n", getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.Actions.BeforeSnapshotRoot == h
		}))

		printActionCommand(h)

		anyActions = true
	}

	if h := p.Actions.AfterSnapshotRoot; h != nil {
		printStdout("Run command after snapshot root:   %v\n", getDefinitionPoint(p.Target(), parents, func(pol *policy.Policy) bool {
			return pol.Actions.AfterSnapshotRoot == h
		}))
		printActionCommand(h)

		anyActions = true
	}

	if h := p.Actions.BeforeFolder; h != nil {
		printStdout("Run command before this folder:    (non-inheritable)\n")

		printActionCommand(h)

		anyActions = true
	}

	if h := p.Actions.AfterFolder; h != nil {
		printStdout("Run command after this folder:    (non-inheritable)\n")
		printActionCommand(h)

		anyActions = true
	}

	if !anyActions {
		printStdout("No actions defined.\n")
	}
}

func printActionCommand(h *policy.ActionCommand) {
	if h.Script != "" {
		printStdout("  Embedded Script: %q\n", h.Script)
	} else {
		printStdout("  Command: %v %v\n", h.Command, strings.Join(h.Arguments, " "))
	}

	printStdout("  Mode: %v\n", h.Mode)
	printStdout("  Timeout: %v\n", h.TimeoutSeconds)
	printStdout("\n")
}

func valueOrNotSet(p *int) string {
	if p == nil {
		return "-"
	}

	return fmt.Sprintf("%v", *p)
}
