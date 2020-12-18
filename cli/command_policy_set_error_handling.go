package cli

import (
	"context"

	"github.com/kopia/kopia/snapshot/policy"
)

var (
	// Error handling behavior.
	policyIgnoreFileErrors      = policySetCommand.Flag("ignore-file-errors", "Ignore errors reading files while traversing ('true', 'false', 'inherit')").Enum(booleanEnumValues...)
	policyIgnoreDirectoryErrors = policySetCommand.Flag("ignore-dir-errors", "Ignore errors reading directories while traversing ('true', 'false', 'inherit").Enum(booleanEnumValues...)
)

func setErrorHandlingPolicyFromFlags(ctx context.Context, fp *policy.ErrorHandlingPolicy, changeCount *int) error {
	if err := applyPolicyBoolPtr(ctx, "ignore file read errors", &fp.IgnoreFileErrors, *policyIgnoreFileErrors, changeCount); err != nil {
		return err
	}

	if err := applyPolicyBoolPtr(ctx, "ignore dir read errors", &fp.IgnoreDirectoryErrors, *policyIgnoreDirectoryErrors, changeCount); err != nil {
		return err
	}

	return nil
}
