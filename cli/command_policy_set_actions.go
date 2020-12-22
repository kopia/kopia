package cli

import (
	"context"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/snapshot/policy"
)

const maxScriptLength = 32000

var (
	policySetBeforeFolderActionCommand       = policySetCommand.Flag("before-folder-action", "Path to before-folder action command ('none' to remove)").Default("-").PlaceHolder("COMMAND").String()
	policySetAfterFolderActionCommand        = policySetCommand.Flag("after-folder-action", "Path to after-folder action command ('none' to remove)").Default("-").PlaceHolder("COMMAND").String()
	policySetBeforeSnapshotRootActionCommand = policySetCommand.Flag("before-snapshot-root-action", "Path to before-snapshot-root action command ('none' to remove or 'inherit')").Default("-").PlaceHolder("COMMAND").String()
	policySetAfterSnapshotRootActionCommand  = policySetCommand.Flag("after-snapshot-root-action", "Path to after-snapshot-root action command ('none' to remove or 'inherit')").Default("-").PlaceHolder("COMMAND").String()
	policySetActionCommandTimeout            = policySetCommand.Flag("action-command-timeout", "Max time allowed for a action to run in seconds").Default("5m").Duration()
	policySetActionCommandMode               = policySetCommand.Flag("action-command-mode", "Action command mode").Default("essential").Enum("essential", "optional", "async")
	policySetPersistActionScript             = policySetCommand.Flag("persist-action-script", "Persist action script").Bool()
)

func setActionsFromFlags(ctx context.Context, p *policy.ActionsPolicy, changeCount *int) error {
	if err := setActionCommandFromFlags(ctx, "before-folder", &p.BeforeFolder, *policySetBeforeFolderActionCommand, changeCount); err != nil {
		return errors.Wrap(err, "invalid before-folder-action")
	}

	if err := setActionCommandFromFlags(ctx, "after-folder", &p.AfterFolder, *policySetAfterFolderActionCommand, changeCount); err != nil {
		return errors.Wrap(err, "invalid after-folder-action")
	}

	if err := setActionCommandFromFlags(ctx, "before-snapshot-root", &p.BeforeSnapshotRoot, *policySetBeforeSnapshotRootActionCommand, changeCount); err != nil {
		return errors.Wrap(err, "invalid before-snapshot-root-action")
	}

	if err := setActionCommandFromFlags(ctx, "after-snapshot-root", &p.AfterSnapshotRoot, *policySetAfterSnapshotRootActionCommand, changeCount); err != nil {
		return errors.Wrap(err, "invalid after-snapshot-root-action")
	}

	return nil
}

func setActionCommandFromFlags(ctx context.Context, actionName string, cmd **policy.ActionCommand, value string, changeCount *int) error {
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
		TimeoutSeconds: int(policySetActionCommandTimeout.Seconds()),
		Mode:           *policySetActionCommandMode,
	}

	*changeCount++

	if *policySetPersistActionScript {
		script, err := ioutil.ReadFile(value) //nolint:gosec
		if err != nil {
			return errors.Wrap(err, "unable to read script file")
		}

		if len(script) > maxScriptLength {
			return errors.Errorf("action script file (%v) too long: %v, max allowed %d", value, len(script), maxScriptLength)
		}

		log(ctx).Infof(" - setting %v (%v) action script from file %v (%v bytes) with timeout %v", actionName, *policySetActionCommandMode, value, len(script), *policySetActionCommandTimeout)

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
		log(ctx).Infof(" - setting %v (%v) action command to %v and timeout %v", actionName, *policySetActionCommandMode, quoteArguments((*cmd).Command), *policySetActionCommandTimeout)
	} else {
		log(ctx).Infof(" - setting %v (%v) action command to %v with arguments %v and timeout %v", actionName, *policySetActionCommandMode, quoteArguments((*cmd).Command), quoteArguments((*cmd).Arguments...), *policySetActionCommandTimeout)
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
