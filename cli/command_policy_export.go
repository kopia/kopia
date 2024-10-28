package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/impossible"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/policy"
)

type commandPolicyExport struct {
	policyTargetFlags
	filePath  string
	overwrite bool

	jsonIndent bool

	svc appServices
}

const exportFilePerms = 0o600

func (c *commandPolicyExport) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("export", "Exports the policy to the specified file, or to stdout if none is specified.")
	cmd.Flag("to-file", "File path to export to").StringVar(&c.filePath)
	cmd.Flag("overwrite", "Overwrite the file if it exists").BoolVar(&c.overwrite)

	cmd.Flag("json-indent", "Output result in indented JSON format").Hidden().BoolVar(&c.jsonIndent)

	c.policyTargetFlags.setup(cmd)

	c.svc = svc

	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandPolicyExport) run(ctx context.Context, rep repo.Repository) error {
	output, err := getOutput(c)
	if err != nil {
		return err
	}

	file, ok := output.(*os.File)
	if ok {
		defer file.Close() //nolint:errcheck
	}

	policies := make(map[string]*policy.Policy)

	if c.policyTargetFlags.global || len(c.policyTargetFlags.targets) > 0 {
		targets, err := c.policyTargets(ctx, rep)
		if err != nil {
			return err
		}

		for _, target := range targets {
			definedPolicy, err := policy.GetDefinedPolicy(ctx, rep, target)
			if err != nil {
				return errors.Wrapf(err, "can't get defined policy for %q", target)
			}

			policies[target.String()] = definedPolicy
		}
	} else {
		ps, err := policy.ListPolicies(ctx, rep)
		if err != nil {
			return errors.Wrap(err, "failed to list policies")
		}

		for _, policy := range ps {
			policies[policy.Target().String()] = policy
		}
	}

	var toWrite []byte

	if c.jsonIndent {
		toWrite, err = json.MarshalIndent(policies, "", "  ")
	} else {
		toWrite, err = json.Marshal(policies)
	}

	impossible.PanicOnError(err)

	_, err = fmt.Fprintf(output, "%s", toWrite)

	return errors.Wrap(err, "unable to write policy to output")
}

func getOutput(c *commandPolicyExport) (io.Writer, error) {
	var err error

	if c.filePath == "" {
		if c.overwrite {
			return nil, errors.New("overwrite was passed but no file path was given")
		}

		return c.svc.stdout(), nil
	}

	var file *os.File

	if c.overwrite {
		file, err = os.Create(c.filePath)
	} else {
		file, err = os.OpenFile(c.filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, exportFilePerms)
		if os.IsExist(err) {
			return nil, errors.Wrap(err, "file already exists and overwrite flag is not set")
		}
	}

	if err != nil {
		return nil, errors.Wrap(err, "error opening file to write to")
	}

	return file, nil
}
