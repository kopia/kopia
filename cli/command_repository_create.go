package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/storage"
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
	ctx, cancel := context.WithCancel(ctx)
	ch := s.ListBlocks(ctx, "")
	d, hasData := <-ch
	cancel()

	if hasData {
		if d.Error != nil {
			return d.Error
		}

		if !*createOverwrite {
			return fmt.Errorf("found existing data in storage, specify --overwrite to use anyway")
		}
	}

	return nil
}

func runCreateCommandWithStorage(ctx context.Context, st storage.Storage) error {
	err := ensureEmpty(ctx, st)
	if err != nil {
		return fmt.Errorf("unable to get repository storage: %v", err)
	}

	options := newRepositoryOptionsFromFlags()

	creds, err := getRepositoryCredentials(true)
	if err != nil {
		return fmt.Errorf("unable to get credentials: %v", err)
	}

	fmt.Fprintf(os.Stderr, "Initializing repository with:\n")
	fmt.Fprintf(os.Stderr, "  metadata encryption: %v\n", options.MetadataEncryptionAlgorithm)
	fmt.Fprintf(os.Stderr, "  block format:        %v\n", options.BlockFormat)
	switch options.Splitter {
	case "DYNAMIC":
		fmt.Fprintf(os.Stderr, "  object splitter:     DYNAMIC with block sizes (min:%v avg:%v max:%v)\n",
			units.BytesStringBase2(int64(options.MinBlockSize)),
			units.BytesStringBase2(int64(options.AvgBlockSize)),
			units.BytesStringBase2(int64(options.MaxBlockSize)))

	case "FIXED":
		fmt.Fprintf(os.Stderr, "  object splitter:     FIXED with with block size: %v\n", units.BytesStringBase2(int64(options.MaxBlockSize)))

	case "NEVER":
		fmt.Fprintf(os.Stderr, "  object splitter:     NEVER\n")
	}

	if err := repo.Initialize(ctx, st, options, creds); err != nil {
		return fmt.Errorf("cannot initialize repository: %v", err)
	}

	if !*createOnly {
		err := repo.Connect(ctx, repositoryConfigFileName(), st, creds, connectOptions())
		if err != nil {
			return err
		}

		fmt.Fprintln(os.Stderr, "Connected to repository")
	}

	return nil
}
