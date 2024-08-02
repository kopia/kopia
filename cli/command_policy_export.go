package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/pkg/errors"
)

type commandPolicyExport struct {
	policyTargetFlags
	filePath  string
	overwrite bool

	jsonIndent bool

	stdout io.Writer
}

func (c *commandPolicyExport) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("export", "Export snapshot policy as json.")
	cmd.Flag("to-file", "Writes the policy to the specified file. Uses stdout otherwise").StringVar(&c.filePath)
	cmd.Flag("overwrite", "Overwrite the file if it exists").BoolVar(&c.overwrite)

	cmd.Flag("json-indent", "Output result in indented JSON format").Hidden().BoolVar(&c.jsonIndent)

	c.policyTargetFlags.setup(cmd)

	c.stdout = svc.stdout()

	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandPolicyExport) run(ctx context.Context, rep repo.Repository) error {
	var output io.Writer
	var err error
	var deferErr error

	if c.filePath != "" {
		var file *os.File

		if c.overwrite {
			file, err = os.Create(c.filePath)
		} else {
			file, err = os.OpenFile(c.filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
			if os.IsExist(err) {
				return errors.Wrap(err, "file already exists and overwrite flag is not set")
			}
		}

		if err != nil {
			return errors.Wrap(err, "error opening file to write to")
		}

		wr := bufio.NewWriter(file)
		defer func() {
			if deferErr = wr.Flush(); deferErr != nil {
				deferErr = errors.Wrap(deferErr, "failed to flush file")
				return
			}
			if deferErr = file.Sync(); deferErr != nil {
				deferErr = errors.Wrap(deferErr, "failed to sync file")
				return
			}
			if deferErr = file.Close(); deferErr != nil {
				deferErr = errors.Wrap(deferErr, "failed to close file")
				return
			}
		}()

		output = wr
	} else {
		if c.overwrite {
			return errors.New("overwrite was passed but no file path was given")
		}
		output = c.stdout
	}

	policies := make(map[string]*policy.Policy)

	if c.policyTargetFlags.global || len(c.policyTargetFlags.targets) > 0 {
		targets, err := c.policyTargets(ctx, rep)
		if err != nil {
			return err
		}

		for _, target := range targets {
			policy, err := policy.GetDefinedPolicy(ctx, rep, target)
			if err != nil {
				return errors.Wrapf(err, "can't get defined policy for %q", target)
			}

			policies[target.String()] = policy
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

	if err != nil {
		panic("error serializing JSON, that should not happen: " + err.Error())
	}

	fmt.Fprintf(output, "%s", toWrite) //nolint:errcheck

	return nil
}
