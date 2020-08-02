package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/repo/splitter"
	"github.com/kopia/kopia/snapshot/policy"
)

var (
	createCommand = repositoryCommands.Command("create", "Create new repository in a specified location.")

	createBlockHashFormat       = createCommand.Flag("block-hash", "Content hash algorithm.").PlaceHolder("ALGO").Default(hashing.DefaultAlgorithm).Enum(hashing.SupportedAlgorithms()...)
	createBlockEncryptionFormat = createCommand.Flag("encryption", "Content encryption algorithm.").PlaceHolder("ALGO").Default(encryption.DefaultAlgorithm).Enum(encryption.SupportedAlgorithms(false)...)
	createSplitter              = createCommand.Flag("object-splitter", "The splitter to use for new objects in the repository").Default(splitter.DefaultAlgorithm).Enum(splitter.SupportedAlgorithms()...)

	createOnly = createCommand.Flag("create-only", "Create repository, but don't connect to it.").Short('c').Bool()
)

func init() {
	setupConnectOptions(createCommand)
}

func newRepositoryOptionsFromFlags() *repo.NewRepositoryOptions {
	return &repo.NewRepositoryOptions{
		BlockFormat: content.FormattingOptions{
			Hash:       *createBlockHashFormat,
			Encryption: *createBlockEncryptionFormat,
		},

		ObjectFormat: object.Format{
			Splitter: *createSplitter,
		},
	}
}

func ensureEmpty(ctx context.Context, s blob.Storage) error {
	hasDataError := errors.New("has data")

	err := s.ListBlobs(ctx, "", func(cb blob.Metadata) error {
		return hasDataError
	})
	if err == hasDataError {
		return errors.New("found existing data in storage location")
	}

	return err
}

func runCreateCommandWithStorage(ctx context.Context, st blob.Storage) error {
	err := ensureEmpty(ctx, st)
	if err != nil {
		return errors.Wrap(err, "unable to get repository storage")
	}

	options := newRepositoryOptionsFromFlags()

	password, err := getPasswordFromFlags(ctx, true, false)
	if err != nil {
		return errors.Wrap(err, "getting password")
	}

	printStderr("Initializing repository with:\n")
	printStderr("  block hash:          %v\n", options.BlockFormat.Hash)
	printStderr("  encryption:          %v\n", options.BlockFormat.Encryption)
	printStderr("  splitter:            %v\n", options.ObjectFormat.Splitter)

	if err := repo.Initialize(ctx, st, options, password); err != nil {
		return errors.Wrap(err, "cannot initialize repository")
	}

	if *createOnly {
		return nil
	}

	if err := runConnectCommandWithStorageAndPassword(ctx, st, password); err != nil {
		return errors.Wrap(err, "unable to connect to repository")
	}

	return populateRepository(ctx, password)
}

func populateRepository(ctx context.Context, password string) error {
	rep, err := repo.Open(ctx, repositoryConfigFileName(), password, applyOptionsFromFlags(ctx, nil))
	if err != nil {
		return errors.Wrap(err, "unable to open repository")
	}
	defer rep.Close(ctx) //nolint:errcheck

	if err := policy.SetPolicy(ctx, rep, policy.GlobalPolicySourceInfo, policy.DefaultPolicy); err != nil {
		return errors.Wrap(err, "unable to set global policy")
	}

	printPolicy(policy.DefaultPolicy, nil)
	printStdout("\n")
	printStdout("To change the policy use:\n  kopia policy set --global <options>\n")
	printStdout("or\n  kopia policy set <dir> <options>\n")

	return setDefaultMaintenanceParameters(ctx, rep.(maintenance.MaintainableRepository))
}
