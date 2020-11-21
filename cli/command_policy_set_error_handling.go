package cli

import (
	"context"
	"strconv"

	"github.com/kopia/kopia/snapshot/policy"
)

var (
	// Error handling behavior.
	policyIgnoreFileErrors      = policySetCommand.Flag("ignore-file-errors", "Ignore errors reading files while traversing ('true', 'false', 'inherit')").Enum(booleanEnumValues...)
	policyIgnoreDirectoryErrors = policySetCommand.Flag("ignore-dir-errors", "Ignore errors reading directories while traversing ('true', 'false', 'inherit").Enum(booleanEnumValues...)
)

func setErrorHandlingPolicyFromFlags(ctx context.Context, fp *policy.ErrorHandlingPolicy, changeCount *int) error {
	switch {
	case *policyIgnoreFileErrors == "":
	case *policyIgnoreFileErrors == inheritPolicyString:
		*changeCount++

		fp.IgnoreFileErrors = nil

		log(ctx).Infof(" - inherit file read error behavior from parent\n")
	default:
		val, err := strconv.ParseBool(*policyIgnoreFileErrors)
		if err != nil {
			return err
		}

		*changeCount++

		fp.IgnoreFileErrors = &val

		log(ctx).Infof(" - setting ignore file read errors to %v\n", val)
	}

	switch {
	case *policyIgnoreDirectoryErrors == "":
	case *policyIgnoreDirectoryErrors == inheritPolicyString:
		*changeCount++

		fp.IgnoreDirectoryErrors = nil

		log(ctx).Infof(" - inherit directory read error behavior from parent\n")
	default:
		val, err := strconv.ParseBool(*policyIgnoreDirectoryErrors)
		if err != nil {
			return err
		}

		*changeCount++

		fp.IgnoreDirectoryErrors = &val

		log(ctx).Infof(" - setting ignore directory read errors to %v\n", val)
	}

	return nil
}
