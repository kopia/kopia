package cli

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

var (
	policyShowCommand = policyCommands.Command("show", "Show snapshot policy.").Alias("get")
	policyShowGlobal  = policyShowCommand.Flag("global", "Get global policy").Bool()
	policyShowTargets = policyShowCommand.Arg("target", "Target to show the policy for").Strings()
	policyShowJSON    = policyShowCommand.Flag("json", "Show JSON").Short('j').Bool()
)

func init() {
	policyShowCommand.Action(repositoryAction(showPolicy))
}

func showPolicy(ctx context.Context, rep *repo.Repository) error {
	pmgr := snapshot.NewPolicyManager(rep)

	targets, err := policyTargets(pmgr, policyShowGlobal, policyShowTargets)
	if err != nil {
		return err
	}

	for _, target := range targets {
		effective, policies, err := pmgr.GetEffectivePolicy(target)
		if err != nil {
			return fmt.Errorf("can't get effective policy for %q: %v", target, err)
		}

		if *policyShowJSON {
			fmt.Println(effective)
		} else {
			printPolicy(os.Stdout, effective, policies)
		}
	}

	return nil
}

func getDefinitionPoint(parents []*snapshot.Policy, match func(p *snapshot.Policy) bool) string {
	for i, p := range parents {
		if match(p) {
			if i == 0 {
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

func printPolicy(w io.Writer, p *snapshot.Policy, parents []*snapshot.Policy) {
	fmt.Fprintf(w, "Policy for %v:\n", p.Target())

	printRetentionPolicy(w, p, parents)
	fmt.Fprintf(w, "\n")
	printFilesPolicy(w, p, parents)
	fmt.Fprintf(w, "\n")
	printSchedulingPolicy(w, p, parents)
}

func printRetentionPolicy(w io.Writer, p *snapshot.Policy, parents []*snapshot.Policy) {
	fmt.Fprintf(w, "Keep:\n")
	fmt.Fprintf(w, "  Annual snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepAnnual),
		getDefinitionPoint(parents, func(pol *snapshot.Policy) bool {
			return pol.RetentionPolicy.KeepAnnual != nil
		}))
	fmt.Fprintf(w, "  Monthly snapshots: %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepMonthly),
		getDefinitionPoint(parents, func(pol *snapshot.Policy) bool {
			return pol.RetentionPolicy.KeepMonthly != nil
		}))
	fmt.Fprintf(w, "  Weekly snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepWeekly),
		getDefinitionPoint(parents, func(pol *snapshot.Policy) bool {
			return pol.RetentionPolicy.KeepWeekly != nil
		}))
	fmt.Fprintf(w, "  Daily snapshots:   %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepDaily),
		getDefinitionPoint(parents, func(pol *snapshot.Policy) bool {
			return pol.RetentionPolicy.KeepDaily != nil
		}))
	fmt.Fprintf(w, "  Hourly snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepHourly),
		getDefinitionPoint(parents, func(pol *snapshot.Policy) bool {
			return pol.RetentionPolicy.KeepHourly != nil
		}))
	fmt.Fprintf(w, "  Latest snapshots:  %3v           %v\n",
		valueOrNotSet(p.RetentionPolicy.KeepLatest),
		getDefinitionPoint(parents, func(pol *snapshot.Policy) bool {
			return pol.RetentionPolicy.KeepLatest != nil
		}))
}

func printFilesPolicy(w io.Writer, p *snapshot.Policy, parents []*snapshot.Policy) {
	fmt.Fprintf(w, "Files policy:\n")

	if len(p.FilesPolicy.Include) == 0 {
		fmt.Fprintf(w, "  Include all files.\n")
	} else {
		fmt.Fprintf(w, "  Include only:\n")
	}
	for _, inc := range p.FilesPolicy.Include {
		fmt.Fprintf(w, "    %-30v %v\n", inc, getDefinitionPoint(parents, func(pol *snapshot.Policy) bool {
			return containsString(pol.FilesPolicy.Include, inc)
		}))
	}
	if len(p.FilesPolicy.Exclude) > 0 {
		fmt.Fprintf(w, "  Exclude:\n")
		for _, exc := range p.FilesPolicy.Exclude {
			fmt.Fprintf(w, "    %-30v %v\n", exc, getDefinitionPoint(parents, func(pol *snapshot.Policy) bool {
				return containsString(pol.FilesPolicy.Exclude, exc)
			}))
		}
	} else {
		fmt.Fprintf(w, "  No excluded files.\n")
	}
	if s := p.FilesPolicy.MaxSize; s != nil {
		fmt.Fprintf(w, "  Exclude files above: %10v  %v\n",
			units.BytesStringBase2(int64(*s)),
			getDefinitionPoint(parents, func(pol *snapshot.Policy) bool {
				return pol.FilesPolicy.MaxSize != nil
			}))
	}
}

func printSchedulingPolicy(w io.Writer, p *snapshot.Policy, parents []*snapshot.Policy) {
	if p.SchedulingPolicy.Interval != nil {
		fmt.Fprintf(w, "Snapshot interval:     %10v  %v\n", p.SchedulingPolicy.Interval, getDefinitionPoint(parents, func(pol *snapshot.Policy) bool {
			return pol.SchedulingPolicy.Interval != nil
		}))
	}
	if len(p.SchedulingPolicy.TimesOfDay) > 0 {
		fmt.Fprintf(w, "Snapshot times:\n")
		for _, tod := range p.SchedulingPolicy.TimesOfDay {
			fmt.Fprintf(w, "  %9v                        %v\n", tod, getDefinitionPoint(parents, func(pol *snapshot.Policy) bool {
				for _, t := range pol.SchedulingPolicy.TimesOfDay {
					if t == tod {
						return true
					}
				}

				return false
			}))
		}
	}
}

func valueOrNotSet(p *int) string {
	if p == nil {
		return "-"
	}

	return fmt.Sprintf("%v", *p)
}
