package cli

import (
	"context"
	"sort"
	"strconv"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/snapshot/policy"
)

type commandPolicySet struct {
	policyTargetFlags
	inherit []bool // not really a list, just an optional boolean

	policyActionFlags
	policyCompressionFlags
	policyMetadataCompressionFlags
	policySplitterFlags
	policyErrorFlags
	policyFilesFlags
	policyLoggingFlags
	policyRetentionFlags
	policySchedulingFlags
	policyOSSnapshotFlags
	policyUploadFlags
}

func (c *commandPolicySet) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("set", "Set snapshot policy for a single directory, user@host or a global policy.")
	c.policyTargetFlags.setup(cmd)
	cmd.Flag(inheritPolicyString, "Enable or disable inheriting policies from the parent").BoolListVar(&c.inherit)

	c.policyActionFlags.setup(cmd)
	c.policyCompressionFlags.setup(cmd)
	c.policyMetadataCompressionFlags.setup(cmd)
	c.policySplitterFlags.setup(cmd)
	c.policyErrorFlags.setup(cmd)
	c.policyFilesFlags.setup(cmd)
	c.policyLoggingFlags.setup(cmd)
	c.policyRetentionFlags.setup(cmd)
	c.policySchedulingFlags.setup(cmd)
	c.policyOSSnapshotFlags.setup(cmd)
	c.policyUploadFlags.setup(cmd)

	cmd.Action(svc.repositoryWriterAction(c.run))
}

//nolint:gochecknoglobals
var booleanEnumValues = []string{"true", "false", "inherit"}

const (
	inheritPolicyString = "inherit"
	defaultPolicyString = "default"
)

func (c *commandPolicySet) run(ctx context.Context, rep repo.RepositoryWriter) error {
	targets, err := c.policyTargets(ctx, rep)
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

		log(ctx).Infof("Setting policy for %v", target)

		changeCount := 0
		if err := c.setPolicyFromFlags(ctx, p, &changeCount); err != nil {
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

func (c *commandPolicySet) setPolicyFromFlags(ctx context.Context, p *policy.Policy, changeCount *int) error {
	if err := c.setRetentionPolicyFromFlags(ctx, &p.RetentionPolicy, changeCount); err != nil {
		return errors.Wrap(err, "retention policy")
	}

	if err := c.setFilesPolicyFromFlags(ctx, &p.FilesPolicy, changeCount); err != nil {
		return errors.Wrap(err, "files policy")
	}

	if err := c.setErrorHandlingPolicyFromFlags(ctx, &p.ErrorHandlingPolicy, changeCount); err != nil {
		return errors.Wrap(err, "error handling policy")
	}

	if err := c.setCompressionPolicyFromFlags(ctx, &p.CompressionPolicy, changeCount); err != nil {
		return errors.Wrap(err, "compression policy")
	}

	if err := c.setMetadataCompressionPolicyFromFlags(ctx, &p.MetadataCompressionPolicy, changeCount); err != nil {
		return errors.Wrap(err, "metadata compression policy")
	}

	if err := c.setSplitterPolicyFromFlags(ctx, &p.SplitterPolicy, changeCount); err != nil {
		return errors.Wrap(err, "splitter policy")
	}

	if err := c.setSchedulingPolicyFromFlags(ctx, &p.SchedulingPolicy, changeCount); err != nil {
		return errors.Wrap(err, "scheduling policy")
	}

	if err := c.setActionsFromFlags(ctx, &p.Actions, changeCount); err != nil {
		return errors.Wrap(err, "actions policy")
	}

	if err := c.setOSSnapshotPolicyFromFlags(ctx, &p.OSSnapshotPolicy, changeCount); err != nil {
		return errors.Wrap(err, "OS snapshot policy")
	}

	if err := c.setLoggingPolicyFromFlags(ctx, &p.LoggingPolicy, changeCount); err != nil {
		return errors.Wrap(err, "actions policy")
	}

	if err := c.setUploadPolicyFromFlags(ctx, &p.UploadPolicy, changeCount); err != nil {
		return errors.Wrap(err, "upload policy")
	}

	// It's not really a list, just optional boolean, last one wins.
	for _, inherit := range c.inherit {
		*changeCount++

		p.NoParent = !inherit
	}

	return nil
}

func applyPolicyStringList(ctx context.Context, desc string, val *[]string, add, remove []string, clearList bool, changeCount *int) {
	if clearList {
		log(ctx).Infof(" - removing all from %q", desc)

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

		log(ctx).Infof(" - adding %q to %q", b, desc)

		entries[b] = true
	}

	for _, b := range remove {
		*changeCount++

		log(ctx).Infof(" - removing %q from %q", b, desc)
		delete(entries, b)
	}

	var s []string
	for k := range entries {
		s = append(s, k)
	}

	sort.Strings(s)

	*val = s
}

func applyOptionalInt(ctx context.Context, desc string, val **policy.OptionalInt, str string, changeCount *int) error {
	if str == "" {
		// not changed
		return nil
	}

	if str == inheritPolicyString || str == defaultPolicyString {
		*changeCount++

		log(ctx).Infof(" - resetting %q to a default value inherited from parent.", desc)

		*val = nil

		return nil
	}

	v, err := strconv.ParseInt(str, 10, 32)
	if err != nil {
		return errors.Wrapf(err, "can't parse the %v %q", desc, str)
	}

	i := policy.OptionalInt(v)
	*changeCount++

	log(ctx).Infof(" - setting %q to %v.", desc, i)
	*val = &i

	return nil
}

func applyOptionalInt64MiB(ctx context.Context, desc string, val **policy.OptionalInt64, str string, changeCount *int) error {
	if str == "" {
		// not changed
		return nil
	}

	if str == inheritPolicyString || str == defaultPolicyString {
		*changeCount++

		log(ctx).Infof(" - resetting %q to a default value inherited from parent.", desc)

		*val = nil

		return nil
	}

	v, err := strconv.ParseInt(str, 10, 32)
	if err != nil {
		return errors.Wrapf(err, "can't parse the %v %q", desc, str)
	}

	// convert MiB to bytes
	v *= 1 << 20 //nolint:mnd

	i := policy.OptionalInt64(v)
	*changeCount++

	log(ctx).Infof(" - setting %q to %v.", desc, units.BytesString(v))

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

		log(ctx).Infof(" - resetting %q to a default value inherited from parent.", desc)

		*val = 0

		return nil
	}

	v, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return errors.Wrapf(err, "can't parse the %q %q", desc, str)
	}

	*changeCount++

	log(ctx).Infof(" - setting %q to %v.", desc, v)
	*val = v

	return nil
}

func applyPolicyBoolPtr(ctx context.Context, desc string, val **policy.OptionalBool, str string, changeCount *int) error {
	if str == "" {
		// not changed
		return nil
	}

	if str == inheritPolicyString || str == defaultPolicyString {
		*changeCount++

		log(ctx).Infof(" - resetting %q to a default value inherited from parent.", desc)

		*val = nil

		return nil
	}

	v, err := strconv.ParseBool(str)
	if err != nil {
		return errors.Wrapf(err, "can't parse the %q %q", desc, str)
	}

	*changeCount++

	log(ctx).Infof(" - setting %q to %v.", desc, v)

	ov := policy.OptionalBool(v)
	*val = &ov

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
