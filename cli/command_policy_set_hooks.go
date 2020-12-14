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
	policySetBeforeFolderHookCommand       = policySetCommand.Flag("before-folder-hook", "Path to before-folder hook command ('none' to remove)").Default("-").PlaceHolder("COMMAND").String()
	policySetAfterFolderHookCommand        = policySetCommand.Flag("after-folder-hook", "Path to after-folder hook command ('none' to remove)").Default("-").PlaceHolder("COMMAND").String()
	policySetBeforeSnapshotRootHookCommand = policySetCommand.Flag("before-snapshot-root-hook", "Path to before-snapshot-root hook command ('none' to remove or 'inherit')").Default("-").PlaceHolder("COMMAND").String()
	policySetAfterSnapshotRootHookCommand  = policySetCommand.Flag("after-snapshot-root-hook", "Path to after-snapshot-root hook command ('none' to remove or 'inherit')").Default("-").PlaceHolder("COMMAND").String()
	policySetHookCommandTimeout            = policySetCommand.Flag("hook-command-timeout", "Max time allowed for a hook to run in seconds").Default("5m").Duration()
	policySetHookCommandMode               = policySetCommand.Flag("hook-command-mode", "Hook command mode").Default("essential").Enum("essential", "optional", "async")
	policySetPersistHookScript             = policySetCommand.Flag("persist-hook-script", "Persist hook script").Bool()
)

func setHooksFromFlags(ctx context.Context, p *policy.HooksPolicy, changeCount *int) error {
	if err := setHookCommandFromFlags(ctx, "before-folder", &p.BeforeFolder, *policySetBeforeFolderHookCommand, changeCount); err != nil {
		return errors.Wrap(err, "invalid before-folder-hook")
	}

	if err := setHookCommandFromFlags(ctx, "after-folder", &p.AfterFolder, *policySetAfterFolderHookCommand, changeCount); err != nil {
		return errors.Wrap(err, "invalid after-folder-hook")
	}

	if err := setHookCommandFromFlags(ctx, "before-snapshot-root", &p.BeforeSnapshotRoot, *policySetBeforeSnapshotRootHookCommand, changeCount); err != nil {
		return errors.Wrap(err, "invalid before-snapshot-root-hook")
	}

	if err := setHookCommandFromFlags(ctx, "after-snapshot-root", &p.AfterSnapshotRoot, *policySetAfterSnapshotRootHookCommand, changeCount); err != nil {
		return errors.Wrap(err, "invalid after-snapshot-root-hook")
	}

	return nil
}

func setHookCommandFromFlags(ctx context.Context, hookName string, cmd **policy.HookCommand, value string, changeCount *int) error {
	if value == "-" {
		// not set
		return nil
	}

	if value == "" {
		log(ctx).Infof(" - removing %v hook", hookName)

		*changeCount++

		*cmd = nil

		return nil
	}

	*cmd = &policy.HookCommand{
		TimeoutSeconds: int(policySetHookCommandTimeout.Seconds()),
		Mode:           *policySetHookCommandMode,
	}

	*changeCount++

	if *policySetPersistHookScript {
		script, err := ioutil.ReadFile(value) //nolint:gosec
		if err != nil {
			return err
		}

		if len(script) > maxScriptLength {
			return errors.Errorf("hook script file (%v) too long: %v, max allowed %d", value, len(script), maxScriptLength)
		}

		log(ctx).Infof(" - setting %v (%v) hook script from file %v (%v bytes) with timeout %v", hookName, *policySetHookCommandMode, value, len(script), *policySetHookCommandTimeout)

		(*cmd).Script = string(script)

		return nil
	}

	// parse path as CSV as if space was the separator, this automatically takes care of quotations
	r := csv.NewReader(strings.NewReader(value))
	r.Comma = ' ' // space

	fields, err := r.Read()
	if err != nil {
		return errors.Wrapf(err, "error parsing %v command", hookName)
	}

	(*cmd).Command = fields[0]
	(*cmd).Arguments = fields[1:]

	if len((*cmd).Arguments) == 0 {
		log(ctx).Infof(" - setting %v (%v) hook command to %v and timeout %v", hookName, *policySetHookCommandMode, quoteArguments((*cmd).Command), *policySetHookCommandTimeout)
	} else {
		log(ctx).Infof(" - setting %v (%v) hook command to %v with arguments %v and timeout %v", hookName, *policySetHookCommandMode, quoteArguments((*cmd).Command), quoteArguments((*cmd).Arguments...), *policySetHookCommandTimeout)
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
