package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/editor"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/policy"
)

const policyEditHelpText = `
# Editing policy for '%v'

# Make changes to the policy, save your file and exit the editor.
# The output must be valid JSON.

# Lines starting with # are comments and automatically removed.

`

const policyEditRetentionHelpText = `  # Retention for snapshots of this directory. Options include:
  #   "keepLatest": number
  #   "keepDaily": number
  #   "keepHourly": number
  #   "keepWeekly": number
  #   "keepMonthly": number
  #   "keepAnnual": number
`

const policyEditFilesHelpText = `
  # Which files to include in snapshots. Options include:
  #   "ignore": ["*.ext", "*.ext2"]
  #   "dotIgnoreFiles": [".gitignore", ".kopiaignore"]
  #   "maxFileSize": number
  #   "noParentDotFiles": true
  #   "noParentIgnore": true
`

const policyEditSchedulingHelpText = `
  # Snapshot scheduling options. Options include:
  #   "intervalSeconds": number /* 86400-day, 3600-hour, 60-minute */
  #   "timesOfDay": [{"hour":H,"min":M},{"hour":H,"min":M}]
`

var (
	policyEditCommand = policyCommands.Command("edit", "Set snapshot policy for a single directory, user@host or a global policy.")
	policyEditTargets = policyEditCommand.Arg("target", "Target of a policy ('global','user@host','@host') or a path").Strings()
	policyEditGlobal  = policyEditCommand.Flag("global", "Set global policy").Bool()
)

func init() {
	policyEditCommand.Action(repositoryAction(editPolicy))
}

func editPolicy(ctx context.Context, rep *repo.Repository) error {
	targets, err := policyTargets(ctx, rep, policyEditGlobal, policyEditTargets)
	if err != nil {
		return err
	}

	for _, target := range targets {
		original, err := policy.GetDefinedPolicy(ctx, rep, target)
		if err == policy.ErrPolicyNotFound {
			original = &policy.Policy{}
		}

		printStderr("Editing policy for %v using external editor...\n", target)

		s := policyEditHelpText + prettyJSON(original)
		s = insertHelpText(s, `  "retention": {`, policyEditRetentionHelpText)
		s = insertHelpText(s, `  "files": {`, policyEditFilesHelpText)
		s = insertHelpText(s, `  "scheduling": {`, policyEditSchedulingHelpText)

		var updated *policy.Policy

		if err := editor.EditLoop(ctx, "policy.conf", s, func(edited string) error {
			updated = &policy.Policy{}
			d := json.NewDecoder(bytes.NewBufferString(edited))
			d.DisallowUnknownFields()
			return d.Decode(updated)
		}); err != nil {
			return err
		}

		if jsonEqual(updated, original) {
			printStderr("Policy for %v unchanged\n", target)
			continue
		}

		printStderr("Updated policy for %v\n%v\n", target, prettyJSON(updated))

		fmt.Print("Save updated policy? (y/N) ")

		var shouldSave string

		fmt.Scanf("%v", &shouldSave) //nolint:errcheck

		if strings.HasPrefix(strings.ToLower(shouldSave), "y") {
			if err := policy.SetPolicy(ctx, rep, target, updated); err != nil {
				return errors.Wrapf(err, "can't save policy for %v", target)
			}
		}
	}

	return nil
}

func prettyJSON(v interface{}) string {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetIndent("", "  ")
	e.Encode(v) //nolint:errcheck

	return b.String()
}

func jsonEqual(v1, v2 interface{}) bool {
	return prettyJSON(v1) == prettyJSON(v2)
}

func insertHelpText(s, lookFor, help string) string {
	p := strings.Index(s, lookFor)
	if p < 0 {
		return s
	}

	return s[0:p] + help + s[p:]
}
