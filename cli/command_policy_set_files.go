package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/snapshot/policy"
)

var (
	// Ignore rules.
	policySetAddIgnore    = policySetCommand.Flag("add-ignore", "List of paths to add to the ignore list").PlaceHolder("PATTERN").Strings()
	policySetRemoveIgnore = policySetCommand.Flag("remove-ignore", "List of paths to remove from the ignore list").PlaceHolder("PATTERN").Strings()
	policySetClearIgnore  = policySetCommand.Flag("clear-ignore", "Clear list of paths in the ignore list").Bool()

	// Dot-ignore files to look at.
	policySetAddDotIgnore    = policySetCommand.Flag("add-dot-ignore", "List of paths to add to the dot-ignore list").PlaceHolder("FILENAME").Strings()
	policySetRemoveDotIgnore = policySetCommand.Flag("remove-dot-ignore", "List of paths to remove from the dot-ignore list").PlaceHolder("FILENAME").Strings()
	policySetClearDotIgnore  = policySetCommand.Flag("clear-dot-ignore", "Clear list of paths in the dot-ignore list").Bool()
	policySetMaxFileSize     = policySetCommand.Flag("max-file-size", "Exclude files above given size").PlaceHolder("N").String()

	// Ignore other mounted fileystems.
	policyOneFileSystem = policySetCommand.Flag("one-file-system", "Stay in parent filesystem when finding files ('true', 'false', 'inherit')").Enum(booleanEnumValues...)

	policyIgnoreCacheDirs = policySetCommand.Flag("ignore-cache-dirs", "Ignore cache directories ('true', 'false', 'inherit')").Enum(booleanEnumValues...)
)

func setFilesPolicyFromFlags(ctx context.Context, fp *policy.FilesPolicy, changeCount *int) error {
	if err := applyPolicyNumber64(ctx, "maximum file size", &fp.MaxFileSize, *policySetMaxFileSize, changeCount); err != nil {
		return errors.Wrap(err, "maximum file size")
	}

	applyPolicyStringList(ctx, "dot-ignore filenames", &fp.DotIgnoreFiles, *policySetAddDotIgnore, *policySetRemoveDotIgnore, *policySetClearDotIgnore, changeCount)
	applyPolicyStringList(ctx, "ignore rules", &fp.IgnoreRules, *policySetAddIgnore, *policySetRemoveIgnore, *policySetClearIgnore, changeCount)

	if err := applyPolicyBoolPtr(ctx, "ignore cache dirs", &fp.IgnoreCacheDirs, *policyIgnoreCacheDirs, changeCount); err != nil {
		return err
	}

	if err := applyPolicyBoolPtr(ctx, "one filesystem", &fp.OneFileSystem, *policyOneFileSystem, changeCount); err != nil {
		return err
	}

	return nil
}
