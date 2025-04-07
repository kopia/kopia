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
  #   "ignoreDotFiles": [".gitignore", ".kopiaignore"]
  #   "maxFileSize": number
  #   "noParentDotFiles": true
  #   "noParentIgnore": true
  #   "oneFileSystem": false
`

const policyEditSchedulingHelpText = `
  # Snapshot scheduling options. Options include:
  #   "intervalSeconds": number /* 86400-day, 3600-hour, 60-minute */
  #   "timeOfDay": [{"hour":H,"min":M},{"hour":H,"min":M}]
  #   "manual": false /* Only create snapshots manually if set to true. NOTE: cannot be used with the above two fields */
`

type commandPolicyEdit struct {
	policyTargetFlags

	out textOutput
}

func (c *commandPolicyEdit) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("edit", "Set snapshot policy for a single directory, user@host or a global policy.")
	c.policyTargetFlags.setup(cmd)
	cmd.Action(svc.repositoryWriterAction(c.run))
	c.out.setup(svc)
}

func (c *commandPolicyEdit) run(ctx context.Context, rep repo.RepositoryWriter) error {
	targets, err := c.policyTargets(ctx, rep)
	if err != nil {
		return err
	}

	for _, target := range targets {
		original, err := policy.GetDefinedPolicy(ctx, rep, target)
		if errors.Is(err, policy.ErrPolicyNotFound) {
			original = &policy.Policy{}
		}

		log(ctx).Infof("Editing policy for %v using external editor...", target)

		s := fmt.Sprintf(policyEditHelpText, target) + prettyJSON(original)
		s = insertHelpText(s, `  "retention": {`, policyEditRetentionHelpText)
		s = insertHelpText(s, `  "files": {`, policyEditFilesHelpText)
		s = insertHelpText(s, `  "scheduling": {`, policyEditSchedulingHelpText)

		var updated *policy.Policy

		if err := editor.EditLoop(ctx, "policy.conf", s, true, func(edited string) error {
			updated = &policy.Policy{}
			d := json.NewDecoder(bytes.NewBufferString(edited))
			d.DisallowUnknownFields()

			return d.Decode(updated)
		}); err != nil {
			return errors.Wrap(err, "unable to launch editor")
		}

		if jsonEqual(updated, original) {
			log(ctx).Infof("Policy for %v unchanged", target)
			continue
		}

		log(ctx).Infof("Updated policy for %v\n%v", target, prettyJSON(updated))

		c.out.printStdout("Save updated policy? (y/N) ")

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

func prettyJSON(v *policy.Policy) string {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetIndent("", "  ")
	e.Encode(v) //nolint:errcheck,errchkjson

	return b.String()
}

func jsonEqual(v1, v2 *policy.Policy) bool {
	return prettyJSON(v1) == prettyJSON(v2)
}

func insertHelpText(s, lookFor, help string) string {
	p := strings.Index(s, lookFor)
	if p < 0 {
		return s
	}

	return s[0:p] + help + s[p:]
}
