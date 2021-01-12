package cli

import (
	"context"
	"sort"
	"strconv"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/snapshot/policy"
)

var booleanEnumValues = []string{"true", "false", "inherit"}

var (
	policySetCommand = policyCommands.Command("set", "Set snapshot policy for a single directory, user@host or a global policy.")
	policySetTargets = policySetCommand.Arg("target", "Target of a policy ('global','user@host','@host') or a path").Strings()
	policySetGlobal  = policySetCommand.Flag("global", "Set global policy").Bool()
	policySetInherit = policySetCommand.Flag(inheritPolicyString, "Enable or disable inheriting policies from the parent").BoolList()
)

const (
	inheritPolicyString = "inherit"
	defaultPolicyString = "default"
)

func init() {
	policySetCommand.Action(repositoryWriterAction(setPolicy))
}

func setPolicy(ctx context.Context, rep repo.Writer) error {
	targets, err := policyTargets(ctx, rep, policySetGlobal, policySetTargets)
	if err != nil {
		return err
	}

	for _, target := range targets {
		p, err := policy.GetDefinedPolicy(ctx, rep, target)

		switch {
		case errors.Is(err, policy.ErrPolicyNotFound):
			p = &policy.Policy{}
		case err != nil:
			return errors.Wrap(err, "could not get defined policy")
		}

		log(ctx).Infof("Setting policy for %v\n", target)

		changeCount := 0
		if err := setPolicyFromFlags(ctx, p, &changeCount); err != nil {
			return err
		}

		if changeCount == 0 {
			return errors.New("no changes specified")
		}

		if err := policy.SetPolicy(ctx, rep, target, p); err != nil {
			return errors.Wrapf(err, "can't save policy for %v", target)
		}
	}

	return nil
}

func setPolicyFromFlags(ctx context.Context, p *policy.Policy, changeCount *int) error {
	if err := setRetentionPolicyFromFlags(ctx, &p.RetentionPolicy, changeCount); err != nil {
		return errors.Wrap(err, "retention policy")
	}

	if err := setFilesPolicyFromFlags(ctx, &p.FilesPolicy, changeCount); err != nil {
		return errors.Wrap(err, "files policy")
	}

	if err := setErrorHandlingPolicyFromFlags(ctx, &p.ErrorHandlingPolicy, changeCount); err != nil {
		return errors.Wrap(err, "error handling policy")
	}

	if err := setCompressionPolicyFromFlags(ctx, &p.CompressionPolicy, changeCount); err != nil {
		return errors.Wrap(err, "compression policy")
	}

	if err := setSchedulingPolicyFromFlags(ctx, &p.SchedulingPolicy, changeCount); err != nil {
		return errors.Wrap(err, "scheduling policy")
	}

	if err := setActionsFromFlags(ctx, &p.Actions, changeCount); err != nil {
		return errors.Wrap(err, "actions policy")
	}

	// It's not really a list, just optional boolean, last one wins.
	for _, inherit := range *policySetInherit {
		*changeCount++

		p.NoParent = !inherit
	}

	return nil
}

func applyPolicyStringList(ctx context.Context, desc string, val *[]string, add, remove []string, clear bool, changeCount *int) {
	if clear {
		log(ctx).Infof(" - removing all from %q\n", desc)

		*changeCount++

		*val = nil

		return
	}

	entries := map[string]bool{}
	for _, b := range *val {
		entries[b] = true
	}

	for _, b := range add {
		*changeCount++

		log(ctx).Infof(" - adding %q to %q\n", b, desc)

		entries[b] = true
	}

	for _, b := range remove {
		*changeCount++

		log(ctx).Infof(" - removing %q from %q\n", b, desc)
		delete(entries, b)
	}

	var s []string
	for k := range entries {
		s = append(s, k)
	}

	sort.Strings(s)

	*val = s
}

func applyPolicyNumber(ctx context.Context, desc string, val **int, str string, changeCount *int) error {
	if str == "" {
		// not changed
		return nil
	}

	if str == inheritPolicyString || str == defaultPolicyString {
		*changeCount++

		log(ctx).Infof(" - resetting %q to a default value inherited from parent.\n", desc)

		*val = nil

		return nil
	}

	v, err := strconv.ParseInt(str, 10, 32)
	if err != nil {
		return errors.Wrapf(err, "can't parse the %v %q", desc, str)
	}

	i := int(v)
	*changeCount++

	log(ctx).Infof(" - setting %q to %v.\n", desc, i)
	*val = &i

	return nil
}

func applyPolicyNumber64(ctx context.Context, desc string, val *int64, str string, changeCount *int) error {
	if str == "" {
		// not changed
		return nil
	}

	if str == inheritPolicyString || str == defaultPolicyString {
		*changeCount++

		log(ctx).Infof(" - resetting %q to a default value inherited from parent.\n", desc)

		*val = 0

		return nil
	}

	v, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return errors.Wrapf(err, "can't parse the %q %q", desc, str)
	}

	*changeCount++

	log(ctx).Infof(" - setting %q to %v.\n", desc, v)
	*val = v

	return nil
}

func applyPolicyBoolPtr(ctx context.Context, desc string, val **bool, str string, changeCount *int) error {
	if str == "" {
		// not changed
		return nil
	}

	if str == inheritPolicyString || str == defaultPolicyString {
		*changeCount++

		log(ctx).Infof(" - resetting %q to a default value inherited from parent.\n", desc)

		*val = nil

		return nil
	}

	v, err := strconv.ParseBool(str)
	if err != nil {
		return errors.Wrapf(err, "can't parse the %q %q", desc, str)
	}

	*changeCount++

	log(ctx).Infof(" - setting %q to %v.\n", desc, v)
	*val = &v

	return nil
}

func supportedCompressionAlgorithms() []string {
	var res []string
	for name := range compression.ByName {
		res = append(res, string(name))
	}

	sort.Strings(res)

	return append([]string{inheritPolicyString, "none"}, res...)
}
