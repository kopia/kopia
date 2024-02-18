package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/snapshot/policy"
)

type policyOSSnapshotFlags struct {
	policyEnableVolumeShadowCopy string
}

func (c *policyOSSnapshotFlags) setup(cmd *kingpin.CmdClause) {
	osSnapshotMode := []string{policy.OSSnapshotNeverString, policy.OSSnapshotAlwaysString, policy.OSSnapshotWhenAvailableString, inheritPolicyString}

	cmd.Flag("enable-volume-shadow-copy", "Enable Volume Shadow Copy snapshots ('never', 'always', 'when-available', 'inherit')").PlaceHolder("MODE").EnumVar(&c.policyEnableVolumeShadowCopy, osSnapshotMode...)
}

func (c *policyOSSnapshotFlags) setOSSnapshotPolicyFromFlags(ctx context.Context, fp *policy.OSSnapshotPolicy, changeCount *int) error {
	if err := applyPolicyOSSnapshotMode(ctx, "enable volume shadow copy", &fp.VolumeShadowCopy.Enable, c.policyEnableVolumeShadowCopy, changeCount); err != nil {
		return errors.Wrap(err, "enable volume shadow copy")
	}

	return nil
}

func applyPolicyOSSnapshotMode(ctx context.Context, desc string, val **policy.OSSnapshotMode, str string, changeCount *int) error {
	if str == "" {
		// not changed
		return nil
	}

	var mode policy.OSSnapshotMode

	switch str {
	case inheritPolicyString, defaultPolicyString:
		*changeCount++

		log(ctx).Infof(" - resetting %q to a default value inherited from parent.", desc)

		*val = nil

		return nil
	case policy.OSSnapshotNeverString:
		mode = policy.OSSnapshotNever
	case policy.OSSnapshotAlwaysString:
		mode = policy.OSSnapshotAlways
	case policy.OSSnapshotWhenAvailableString:
		mode = policy.OSSnapshotWhenAvailable
	default:
		return errors.Errorf("invalid %q mode %q", desc, str)
	}

	*changeCount++

	log(ctx).Infof(" - setting %q to %v.", desc, mode)

	*val = &mode

	return nil
}
