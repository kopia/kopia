package cli

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/snapshot/policy"
)

const maxScriptLength = 32000

type policyActionFlags struct {
	policySetBeforeFolderActionCommand       string
	policySetAfterFolderActionCommand        string
	policySetBeforeSnapshotRootActionCommand string
	policySetAfterSnapshotRootActionCommand  string
	policySetActionCommandTimeout            time.Duration
	policySetActionCommandMode               string
	policySetPersistActionScript             bool
}

func (c *policyActionFlags) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("before-folder-action", "Path to before-folder action command ('none' to remove)").Default("-").PlaceHolder("COMMAND").StringVar(&c.policySetBeforeFolderActionCommand)
	cmd.Flag("after-folder-action", "Path to after-folder action command ('none' to remove)").Default("-").PlaceHolder("COMMAND").StringVar(&c.policySetAfterFolderActionCommand)
	cmd.Flag("before-snapshot-root-action", "Path to before-snapshot-root action command ('none' to remove or 'inherit')").Default("-").PlaceHolder("COMMAND").StringVar(&c.policySetBeforeSnapshotRootActionCommand)
	cmd.Flag("after-snapshot-root-action", "Path to after-snapshot-root action command ('none' to remove or 'inherit')").Default("-").PlaceHolder("COMMAND").StringVar(&c.policySetAfterSnapshotRootActionCommand)
	cmd.Flag("action-command-timeout", "Max time allowed for an action to run in seconds").Default("5m").DurationVar(&c.policySetActionCommandTimeout)
	cmd.Flag("action-command-mode", "Action command mode").Default("essential").EnumVar(&c.policySetActionCommandMode, "essential", "optional", "async")
	cmd.Flag("persist-action-script", "Persist action script").BoolVar(&c.policySetPersistActionScript)
}

func (c *policyActionFlags) setActionsFromFlags(ctx context.Context, p *policy.ActionsPolicy, changeCount *int) error {
	if err := c.setActionCommandFromFlags(ctx, "before-folder", &p.BeforeFolder, c.policySetBeforeFolderActionCommand, changeCount); err != nil {
		return errors.Wrap(err, "invalid before-folder-action")
	}

	if err := c.setActionCommandFromFlags(ctx, "after-folder", &p.AfterFolder, c.policySetAfterFolderActionCommand, changeCount); err != nil {
		return errors.Wrap(err, "invalid after-folder-action")
	}

	if err := c.setActionCommandFromFlags(ctx, "before-snapshot-root", &p.BeforeSnapshotRoot, c.policySetBeforeSnapshotRootActionCommand, changeCount); err != nil {
		return errors.Wrap(err, "invalid before-snapshot-root-action")
	}

	if err := c.setActionCommandFromFlags(ctx, "after-snapshot-root", &p.AfterSnapshotRoot, c.policySetAfterSnapshotRootActionCommand, changeCount); err != nil {
		return errors.Wrap(err, "invalid after-snapshot-root-action")
	}

	return nil
}

func (c *policyActionFlags) setActionCommandFromFlags(ctx context.Context, actionName string, cmd **policy.ActionCommand, value string, changeCount *int) error {
	if value == "-" {
		// not set
		return nil
	}

	if value == "" {
		log(ctx).Infof(" - removing %v action", actionName)

		*changeCount++

		*cmd = nil

		return nil
	}

	*cmd = &policy.ActionCommand{
		TimeoutSeconds: int(c.policySetActionCommandTimeout.Seconds()),
		Mode:           c.policySetActionCommandMode,
	}

	*changeCount++

	if c.policySetPersistActionScript {
		script, err := os.ReadFile(value) //nolint:gosec
		if err != nil {
			return errors.Wrap(err, "unable to read script file")
		}

		if len(script) > maxScriptLength {
			return errors.Errorf("action script file (%v) too long: %v, max allowed %d", value, len(script), maxScriptLength)
		}

		log(ctx).Infof(" - setting %v (%v) action script from file %v (%v bytes) with timeout %v", actionName, c.policySetActionCommandMode, value, len(script), c.policySetActionCommandTimeout)

		(*cmd).Script = string(script)

		return nil
	}

	// parse path as CSV as if space was the separator, this automatically takes care of quotations
	r := csv.NewReader(strings.NewReader(value))
	r.Comma = ' ' // space

	fields, err := r.Read()
	if err != nil {
		return errors.Wrapf(err, "error parsing %v command", actionName)
	}

	(*cmd).Command = fields[0]
	(*cmd).Arguments = fields[1:]

	if len((*cmd).Arguments) == 0 {
		log(ctx).Infof(" - setting %v (%v) action command to %v and timeout %v", actionName, c.policySetActionCommandMode, quoteArguments((*cmd).Command), c.policySetActionCommandTimeout)
	} else {
		log(ctx).Infof(" - setting %v (%v) action command to %v with arguments %v and timeout %v", actionName, c.policySetActionCommandMode, quoteArguments((*cmd).Command), quoteArguments((*cmd).Arguments...), c.policySetActionCommandTimeout)
	}

	return nil
}

func quoteArguments(s ...string) string {
	var result []string

	for _, v := range s {
		result = append(result, fmt.Sprintf("\"%v\"", v))
	}

	return strings.Join(result, " ")
}
