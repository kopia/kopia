package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/snapshot/policy"
)

var (
	// Error handling behavior.
	policyIgnoreFileErrors      = policySetCommand.Flag("ignore-file-errors", "Ignore errors reading files while traversing ('true', 'false', 'inherit')").Enum(booleanEnumValues...)
	policyIgnoreDirectoryErrors = policySetCommand.Flag("ignore-dir-errors", "Ignore errors reading directories while traversing ('true', 'false', 'inherit").Enum(booleanEnumValues...)
	policyIgnoreUnknownTypes    = policySetCommand.Flag("ignore-unknown-types", "Ignore unknown entry types in directories ('true', 'false', 'inherit").Enum(booleanEnumValues...)
)

func setErrorHandlingPolicyFromFlags(ctx context.Context, fp *policy.ErrorHandlingPolicy, changeCount *int) error {
	if err := applyPolicyBoolPtr(ctx, "ignore file errors", &fp.IgnoreFileErrors, *policyIgnoreFileErrors, changeCount); err != nil {
		return errors.Wrap(err, "ignore file errors")
	}

	if err := applyPolicyBoolPtr(ctx, "ignore directory errors", &fp.IgnoreDirectoryErrors, *policyIgnoreDirectoryErrors, changeCount); err != nil {
		return errors.Wrap(err, "ignore directory errors")
	}

	if err := applyPolicyBoolPtr(ctx, "ignore unknown types", &fp.IgnoreUnknownTypes, *policyIgnoreUnknownTypes, changeCount); err != nil {
		return errors.Wrap(err, "ignore unknown types")
	}

	return nil
}
