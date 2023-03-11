package cli

import (
	"context"
	"strconv"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/snapshot/policy"
)

type policyLoggingFlags struct {
	dirSnapshottedDetail   string
	dirIgnoredDetail       string
	entrySnapshottedDetail string
	entryIgnoredDetail     string
	entryCacheHitDetail    string
	entryCacheMissDetail   string
}

func (c *policyLoggingFlags) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("log-dir-snapshotted", "Log detail when a directory is snapshotted (or 'inherit')").PlaceHolder("N").StringVar(&c.dirSnapshottedDetail)
	cmd.Flag("log-dir-ignored", "Log detail when a directory is ignored (or 'inherit')").PlaceHolder("N").StringVar(&c.dirIgnoredDetail)
	cmd.Flag("log-entry-snapshotted", "Log detail when an entry is snapshotted (or 'inherit')").PlaceHolder("N").StringVar(&c.entrySnapshottedDetail)
	cmd.Flag("log-entry-ignored", "Log detail when an entry is ignored (or 'inherit')").PlaceHolder("N").StringVar(&c.entryIgnoredDetail)
	cmd.Flag("log-entry-cache-hit", "Log detail on entry cache hit (or 'inherit')").PlaceHolder("N").StringVar(&c.entryCacheHitDetail)
	cmd.Flag("log-entry-cache-miss", "Log detail on entry cache miss (or 'inherit')").PlaceHolder("N").StringVar(&c.entryCacheMissDetail)
}

func (c *policyLoggingFlags) setLoggingPolicyFromFlags(ctx context.Context, p *policy.LoggingPolicy, changeCount *int) error {
	if err := applyPolicyLogDetailPtr(ctx, "directory snapshotted detail", &p.Directories.Snapshotted, c.dirSnapshottedDetail, changeCount); err != nil {
		return errors.Wrap(err, "directory snapshotted detail")
	}

	if err := applyPolicyLogDetailPtr(ctx, "directory ignored detail", &p.Directories.Ignored, c.dirIgnoredDetail, changeCount); err != nil {
		return errors.Wrap(err, "directory ignored detail")
	}

	if err := applyPolicyLogDetailPtr(ctx, "entry snapshotted detail", &p.Entries.Snapshotted, c.entrySnapshottedDetail, changeCount); err != nil {
		return errors.Wrap(err, "entry snapshotted detail")
	}

	if err := applyPolicyLogDetailPtr(ctx, "entry ignored detail", &p.Entries.Ignored, c.entryIgnoredDetail, changeCount); err != nil {
		return errors.Wrap(err, "entry ignored detail")
	}

	if err := applyPolicyLogDetailPtr(ctx, "entry cache hit detail", &p.Entries.CacheHit, c.entryCacheHitDetail, changeCount); err != nil {
		return errors.Wrap(err, "entry cache hit detail")
	}

	if err := applyPolicyLogDetailPtr(ctx, "entry cache miss detail", &p.Entries.CacheMiss, c.entryCacheMissDetail, changeCount); err != nil {
		return errors.Wrap(err, "entry cache miss detail")
	}

	return nil
}

func applyPolicyLogDetailPtr(ctx context.Context, desc string, val **policy.LogDetail, str string, changeCount *int) error {
	if str == "" {
		// not changed
		return nil
	}

	if str == inheritPolicyString {
		*changeCount++

		log(ctx).Infof(" - resetting %q to a default value inherited from parent.", desc)

		*val = nil

		return nil
	}

	v, err := strconv.Atoi(str)
	if err != nil || v < int(policy.LogDetailNone) || v > int(policy.LogDetailMax) {
		return errors.Errorf("must be >= %v and <= %v or %q", policy.LogDetailNone, policy.LogDetailMax, inheritPolicyString)
	}

	*changeCount++

	log(ctx).Infof(" - setting %q to %v.", desc, v)

	ov := policy.LogDetail(v)
	*val = &ov

	return nil
}
