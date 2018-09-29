package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/kopia/kopia/fs/ignorefs"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/policy"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/block"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/repo/storage"
)

var (
	createCommand = repositoryCommands.Command("create", "Create new repository in a specified location.")

	createMetadataEncryptionFormat = createCommand.Flag("metadata-encryption", "Metadata item encryption.").PlaceHolder("FORMAT").Default(repo.DefaultEncryptionAlgorithm).Enum(repo.SupportedEncryptionAlgorithms...)
	createObjectFormat             = createCommand.Flag("object-format", "Format of repository objects.").PlaceHolder("FORMAT").Default(block.DefaultFormat).Enum(block.SupportedFormats...)
	createObjectSplitter           = createCommand.Flag("object-splitter", "The splitter to use for new objects in the repository").Default("DYNAMIC").Enum(object.SupportedSplitters...)

	createMinBlockSize = createCommand.Flag("min-block-size", "Minimum size of a data block.").PlaceHolder("KB").Default("1024").Int()
	createAvgBlockSize = createCommand.Flag("avg-block-size", "Average size of a data block.").PlaceHolder("KB").Default("10240").Int()
	createMaxBlockSize = createCommand.Flag("max-block-size", "Maximum size of a data block.").PlaceHolder("KB").Default("20480").Int()

	createOverwrite = createCommand.Flag("overwrite", "Overwrite existing data (DANGEROUS).").Bool()
	createOnly      = createCommand.Flag("create-only", "Create repository, but don't connect to it.").Short('c').Bool()

	createGlobalPolicyKeepLatest  = createCommand.Flag("keep-latest", "Number of most recent backups to keep per source").PlaceHolder("N").Default("10").Int()
	createGlobalPolicyKeepHourly  = createCommand.Flag("keep-hourly", "Number of most-recent hourly backups to keep per source").PlaceHolder("N").Default("48").Int()
	createGlobalPolicyKeepDaily   = createCommand.Flag("keep-daily", "Number of most-recent daily backups to keep per source").PlaceHolder("N").Default("14").Int()
	createGlobalPolicyKeepWeekly  = createCommand.Flag("keep-weekly", "Number of most-recent weekly backups to keep per source").PlaceHolder("N").Default("25").Int()
	createGlobalPolicyKeepMonthly = createCommand.Flag("keep-monthly", "Number of most-recent monthly backups to keep per source").PlaceHolder("N").Default("24").Int()
	createGlobalPolicyKeepAnnual  = createCommand.Flag("keep-annual", "Number of most-recent annual backups to keep per source").PlaceHolder("N").Default("3").Int()

	createGlobalPolicyDotIgnoreFiles = createCommand.Flag("dot-ignore", "List of dotfiles to look for ignore rules").Default(".kopiaignore").Strings()

	createGlobalPolicyInterval   = createCommand.Flag("snapshot-interval", "Interval between snapshots").Duration()
	createGlobalPolicyTimesOfDay = createCommand.Flag("snapshot-time", "Times of day when to take snapshot (HH:mm)").Strings()
)

func init() {
	setupConnectOptions(createCommand)
}

func newRepositoryOptionsFromFlags() *repo.NewRepositoryOptions {
	return &repo.NewRepositoryOptions{
		MetadataEncryptionAlgorithm: *createMetadataEncryptionFormat,
		BlockFormat:                 *createObjectFormat,

		Splitter:     *createObjectSplitter,
		MinBlockSize: *createMinBlockSize * 1024,
		AvgBlockSize: *createAvgBlockSize * 1024,
		MaxBlockSize: *createMaxBlockSize * 1024,
	}
}

func ensureEmpty(ctx context.Context, s storage.Storage) error {
	hasDataError := errors.New("has data")
	err := s.ListBlocks(ctx, "", func(cb storage.BlockMetadata) error {
		return hasDataError
	})
	if err == hasDataError {
		if !*createOverwrite {
			return fmt.Errorf("found existing data in storage, specify --overwrite to use anyway")
		}
	}

	return err
}

func runCreateCommandWithStorage(ctx context.Context, st storage.Storage) error {
	err := ensureEmpty(ctx, st)
	if err != nil {
		return fmt.Errorf("unable to get repository storage: %v", err)
	}

	options := newRepositoryOptionsFromFlags()

	password := mustGetPasswordFromFlags(true, false)

	printStderr("Initializing repository with:\n")
	printStderr("  metadata encryption: %v\n", options.MetadataEncryptionAlgorithm)
	printStderr("  block format:        %v\n", options.BlockFormat)
	switch options.Splitter {
	case "DYNAMIC":
		printStderr("  object splitter:     DYNAMIC with block sizes (min:%v avg:%v max:%v)\n",
			units.BytesStringBase2(int64(options.MinBlockSize)),
			units.BytesStringBase2(int64(options.AvgBlockSize)),
			units.BytesStringBase2(int64(options.MaxBlockSize)))

	case "FIXED":
		printStderr("  object splitter:     FIXED with with block size: %v\n", units.BytesStringBase2(int64(options.MaxBlockSize)))

	case "NEVER":
		printStderr("  object splitter:     NEVER\n")
	}

	if err := repo.Initialize(ctx, st, options, password); err != nil {
		return fmt.Errorf("cannot initialize repository: %v", err)
	}

	if *createOnly {
		return nil
	}

	if err := runConnectCommandWithStorageAndPassword(ctx, st, password); err != nil {
		return fmt.Errorf("unable to connect to repository: %v", err)
	}

	return populateRepository(ctx, password)
}

func populateRepository(ctx context.Context, password string) error {
	rep, err := repo.Open(ctx, repositoryConfigFileName(), password, applyOptionsFromFlags(nil))
	if err != nil {
		return fmt.Errorf("unable to open repository: %v", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	globalPolicy, err := getInitialGlobalPolicy()
	if err != nil {
		return fmt.Errorf("unable to initialize global policy: %v", err)
	}

	if err := policy.SetPolicy(ctx, rep, policy.GlobalPolicySourceInfo, globalPolicy); err != nil {
		return fmt.Errorf("unable to set global policy: %v", err)
	}

	printPolicy(globalPolicy, nil)
	return nil
}

func getInitialGlobalPolicy() (*policy.Policy, error) {
	var sp policy.SchedulingPolicy

	sp.SetInterval(*createGlobalPolicyInterval)
	var timesOfDay []policy.TimeOfDay

	for _, tods := range *createGlobalPolicyTimesOfDay {
		for _, tod := range strings.Split(tods, ",") {
			var timeOfDay policy.TimeOfDay
			if err := timeOfDay.Parse(tod); err != nil {
				return nil, fmt.Errorf("unable to parse time of day: %v", err)
			}
			timesOfDay = append(timesOfDay, timeOfDay)
		}
	}
	sp.TimesOfDay = policy.SortAndDedupeTimesOfDay(timesOfDay)

	return &policy.Policy{
		FilesPolicy: ignorefs.FilesPolicy{
			DotIgnoreFiles: *createGlobalPolicyDotIgnoreFiles,
		},
		RetentionPolicy: policy.RetentionPolicy{
			KeepLatest:  createGlobalPolicyKeepLatest,
			KeepHourly:  createGlobalPolicyKeepHourly,
			KeepDaily:   createGlobalPolicyKeepDaily,
			KeepWeekly:  createGlobalPolicyKeepWeekly,
			KeepMonthly: createGlobalPolicyKeepMonthly,
			KeepAnnual:  createGlobalPolicyKeepAnnual,
		},
		SchedulingPolicy: sp,
	}, nil
}
