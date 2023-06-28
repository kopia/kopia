package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/snapshot/policy"
)

type policyErrorFlags struct {
	policyIgnoreFileErrors      string
	policyIgnoreDirectoryErrors string
	policyIgnoreUnknownTypes    string
}

func (c *policyErrorFlags) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("ignore-file-errors", "Ignore errors reading files while traversing ('true', 'false', 'inherit')").EnumVar(&c.policyIgnoreFileErrors, booleanEnumValues...)
	cmd.Flag("ignore-dir-errors", "Ignore errors reading directories while traversing ('true', 'false', 'inherit").EnumVar(&c.policyIgnoreDirectoryErrors, booleanEnumValues...)
	cmd.Flag("ignore-unknown-types", "Ignore unknown entry types in directories ('true', 'false', 'inherit").EnumVar(&c.policyIgnoreUnknownTypes, booleanEnumValues...)
}

func (c *policyErrorFlags) setErrorHandlingPolicyFromFlags(ctx context.Context, fp *policy.ErrorHandlingPolicy, changeCount *int) error {
	if err := applyPolicyBoolPtr(ctx, "ignore file errors", &fp.IgnoreFileErrors, c.policyIgnoreFileErrors, changeCount); err != nil {
		return errors.Wrap(err, "ignore file errors")
	}

	if err := applyPolicyBoolPtr(ctx, "ignore directory errors", &fp.IgnoreDirectoryErrors, c.policyIgnoreDirectoryErrors, changeCount); err != nil {
		return errors.Wrap(err, "ignore directory errors")
	}

	if err := applyPolicyBoolPtr(ctx, "ignore unknown types", &fp.IgnoreUnknownTypes, c.policyIgnoreUnknownTypes, changeCount); err != nil {
		return errors.Wrap(err, "ignore unknown types")
	}

	return nil
}
