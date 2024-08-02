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
	filePath string

	stdin io.Reader
}

func (c *commandPolicyImport) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("import", "Import snapshot policy from json.")
	cmd.Flag("from-file", "Reads the policy from the specified file. Uses stdin otherwise").StringVar(&c.filePath)

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

	for ts, newPolicy := range policies {
		target, err := snapshot.ParseSourceInfo(ts, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
		if err != nil {
			return errors.Wrapf(err, "unable to parse source info: %q", ts)
		}

		if err := policy.SetPolicy(ctx, rep, target, newPolicy); err != nil {
			return errors.Wrapf(err, "can't save policy for %v", target)
		}
	}

	return nil
}
