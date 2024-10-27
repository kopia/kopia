package cli

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"slices"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

type commandPolicyImport struct {
	policyTargetFlags
	filePath            string
	allowUnknownFields  bool
	deleteOtherPolicies bool

	svc appServices
}

func (c *commandPolicyImport) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("import", "Imports policies from a specified file, or stdin if no file is specified.")
	cmd.Flag("from-file", "File path to import from").StringVar(&c.filePath)
	cmd.Flag("allow-unknown-fields", "Allow unknown fields in the policy file").BoolVar(&c.allowUnknownFields)
	cmd.Flag("delete-other-policies", "Delete all other policies, keeping only those that got imported").BoolVar(&c.deleteOtherPolicies)

	c.policyTargetFlags.setup(cmd)
	c.svc = svc

	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandPolicyImport) run(ctx context.Context, rep repo.RepositoryWriter) error {
	var input io.Reader

	var err error

	if c.filePath != "" {
		file, err := os.Open(c.filePath)
		if err != nil {
			return errors.Wrap(err, "unable to read policy file")
		}

		defer file.Close() //nolint:errcheck

		input = file
	} else {
		input = c.svc.stdin()
	}

	policies := make(map[string]*policy.Policy)
	d := json.NewDecoder(input)

	if !c.allowUnknownFields {
		d.DisallowUnknownFields()
	}

	err = d.Decode(&policies)
	if err != nil {
		return errors.Wrap(err, "unable to decode policy file as valid json")
	}

	var targetLimit []snapshot.SourceInfo

	if c.policyTargetFlags.global || len(c.policyTargetFlags.targets) > 0 {
		targetLimit, err = c.policyTargets(ctx, rep)
		if err != nil {
			return err
		}
	}

	shouldImportSource := func(target snapshot.SourceInfo) bool {
		if targetLimit == nil {
			return true
		}

		return slices.Contains(targetLimit, target)
	}

	importedSources := make([]string, 0, len(policies))

	for ts, newPolicy := range policies {
		target, err := snapshot.ParseSourceInfo(ts, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
		if err != nil {
			return errors.Wrapf(err, "unable to parse source info: %q", ts)
		}

		if !shouldImportSource(target) {
			continue
		}
		// used for deleteOtherPolicies
		importedSources = append(importedSources, ts)

		if err := policy.SetPolicy(ctx, rep, target, newPolicy); err != nil {
			return errors.Wrapf(err, "can't save policy for %v", target)
		}
	}

	if c.deleteOtherPolicies {
		err := deleteOthers(ctx, rep, importedSources)
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteOthers(ctx context.Context, rep repo.RepositoryWriter, importedSources []string) error {
	ps, err := policy.ListPolicies(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "failed to list policies")
	}

	for _, p := range ps {
		if !slices.Contains(importedSources, p.Target().String()) {
			if err := policy.RemovePolicy(ctx, rep, p.Target()); err != nil {
				return errors.Wrapf(err, "can't delete policy for %v", p.Target())
			}
		}
	}

	return nil
}
