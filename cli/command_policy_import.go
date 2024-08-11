package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/pkg/errors"
)

type commandPolicyImport struct {
	policyTargetFlags
	filePath string

	stdin io.Reader
}

func (c *commandPolicyImport) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("import", "Imports policies from a specified file, or stdin if no file is specified.")
	cmd.Flag("from-file", "File path to import from").StringVar(&c.filePath)

	c.policyTargetFlags.setup(cmd)
	c.stdin = svc.stdin()

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

		input = bufio.NewReader(file)
	} else {
		input = c.stdin
	}

	policies := make(map[string]*policy.Policy)
	d := json.NewDecoder(input)
	d.DisallowUnknownFields()

	err = d.Decode(&policies)

	if err != nil {
		return errors.Wrap(err, "unable to decode policy file as valid json")
	}

	//var targetLimit []snapshot.SourceInfo
	targetLimit := make(map[snapshot.SourceInfo]struct{})

	if c.policyTargetFlags.global || len(c.policyTargetFlags.targets) > 0 {
		targetLimitSrc, err := c.policyTargets(ctx, rep)
		if err != nil {
			return err
		}

		for _, target := range targetLimitSrc {
			targetLimit[target] = struct{}{}
		}
	}

	for ts, newPolicy := range policies {
		target, err := snapshot.ParseSourceInfo(ts, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
		if err != nil {
			return errors.Wrapf(err, "unable to parse source info: %q", ts)
		}

		if len(targetLimit) > 0 {
			if _, ok := targetLimit[target]; !ok {
				continue
			}
		}

		if err := policy.SetPolicy(ctx, rep, target, newPolicy); err != nil {
			return errors.Wrapf(err, "can't save policy for %v", target)
		}
	}

	return nil
}
