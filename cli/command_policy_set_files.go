package cli

import (
	"context"
	"strconv"

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

	if *policySetClearDotIgnore {
		*changeCount++

		log(ctx).Infof(" - removing all rules for dot-ignore files\n")

		fp.DotIgnoreFiles = nil
	} else {
		fp.DotIgnoreFiles = addRemoveDedupeAndSort(ctx, "dot-ignore files", fp.DotIgnoreFiles, *policySetAddDotIgnore, *policySetRemoveDotIgnore, changeCount)
	}

	if *policySetClearIgnore {
		*changeCount++

		fp.IgnoreRules = nil

		log(ctx).Infof(" - removing all ignore rules\n")
	} else {
		fp.IgnoreRules = addRemoveDedupeAndSort(ctx, "ignored files", fp.IgnoreRules, *policySetAddIgnore, *policySetRemoveIgnore, changeCount)
	}

	switch {
	case *policyIgnoreCacheDirs == "":
	case *policyIgnoreCacheDirs == inheritPolicyString:
		*changeCount++

		fp.IgnoreCacheDirs = nil

		log(ctx).Infof(" - inherit ignoring cache dirs from parent\n")

	default:
		val, err := strconv.ParseBool(*policyIgnoreCacheDirs)
		if err != nil {
			return err
		}

		*changeCount++

		fp.IgnoreCacheDirs = &val

		log(ctx).Infof(" - setting ignore cache dirs to %v\n", val)
	}

	switch {
	case *policyOneFileSystem == "":
	case *policyOneFileSystem == inheritPolicyString:
		*changeCount++

		fp.OneFileSystem = nil

		printStderr(" - inherit one file system from parent\n")

	default:
		val, err := strconv.ParseBool(*policyOneFileSystem)
		if err != nil {
			return err
		}

		*changeCount++

		fp.OneFileSystem = &val

		printStderr(" - setting one file system to %v\n", val)
	}

	return nil
}
