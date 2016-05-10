package main

import (
	"fmt"
	"strings"

	"github.com/kopia/kopia/cas"
	"github.com/kopia/kopia/vault"

	"github.com/kopia/kopia/blob"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	createCommand           = app.Command("create", "Create new vault and optionally connect to it")
	createCommandRepository = createCommand.Flag("repository", "Repository path").Required().String()
	createCommandOnly       = createCommand.Flag("only", "Only create, don't connect.").Bool()

	createMaxBlobSize    = createCommand.Flag("max-blob-size", "Maximum size of a data chunk in bytes.").Default("4000000").Int()
	createInlineBlobSize = createCommand.Flag("inline-blob-size", "Maximum size of an inline data chunk in bytes.").Default("32768").Int()

	createVaultEncryptionFormat = createCommand.Flag("vaultencryption", "Vault encryption format").String()
	createSecurity              = createCommand.Flag("security", "Security mode, one of 'none', 'default' or 'custom'.").Default("default").Enum("none", "default", "custom")
	createCustomFormat          = createCommand.Flag("object-format", "Specifies custom object format to be used").String()
	createOverwrite             = createCommand.Flag("overwrite", "Overwrite existing data.").Bool()
)

func init() {
	createCommand.Action(runCreateCommand)
}

func vaultFormat() *vault.Format {
	f := vault.NewFormat()
	if *createVaultEncryptionFormat != "" {
		f.Encryption = *createVaultEncryptionFormat
	}
	return f
}

func repositoryFormat() (*cas.Format, error) {
	f, err := cas.NewFormat()
	if err != nil {
		return nil, err
	}

	f.MaxBlobSize = *createMaxBlobSize
	f.MaxInlineBlobSize = *createInlineBlobSize

	return f, nil
}

func openStorageAndEnsureEmpty(url string) (blob.Storage, error) {
	s, err := blob.NewStorageFromURL(url)
	if err != nil {
		return nil, err
	}
	ch := s.ListBlocks("")
	_, hasData := <-ch

	if hasData && !*createOverwrite {
		return nil, fmt.Errorf("found existing data in %v, specify --overwrite to use anyway", url)
	}

	return s, nil

}

func runCreateCommand(context *kingpin.ParseContext) error {
	if *vaultPath == "" {
		return fmt.Errorf("--vault is required")
	}
	vaultStorage, err := openStorageAndEnsureEmpty(*vaultPath)
	if err != nil {
		return fmt.Errorf("unable to get vault storage: %v", err)
	}

	repositoryStorage, err := openStorageAndEnsureEmpty(*createCommandRepository)
	if err != nil {
		return fmt.Errorf("unable to get repository storage: %v", err)
	}

	masterKey, password, err := getKeyOrPassword(true)
	if err != nil {
		return fmt.Errorf("unable to get credentials: %v", err)
	}

	var v *vault.Vault
	if masterKey != nil {
		v, err = vault.CreateWithKey(vaultStorage, vaultFormat(), masterKey)
	} else {
		v, err = vault.CreateWithPassword(vaultStorage, vaultFormat(), password)
	}
	if err != nil {
		return fmt.Errorf("cannot create vault: %v", err)
	}

	repoFormat, err := repositoryFormat()
	if err != nil {
		return fmt.Errorf("unable to initialize repository format: %v", err)
	}

	// Make repository to make sure the format is supported.
	_, err = cas.NewRepository(repositoryStorage, repoFormat)
	if err != nil {
		return fmt.Errorf("unable to initialize repository: %v", err)
	}

	v.SetRepository(vault.RepositoryConfig{
		Storage: repositoryStorage.Configuration(),
		Format:  repoFormat,
	})

	if *createCommandOnly {
		fmt.Println("Created vault:", *vaultPath)
		return nil
	}

	persistVaultConfig(v)

	fmt.Println("Created and connected to vault:", *vaultPath)

	return err
}

func getCustomFormat() string {
	if *createCustomFormat != "" {
		if cas.SupportedFormats.Find(*createCustomFormat) == nil {
			fmt.Printf("Format '%s' is not recognized.\n", *createCustomFormat)
		}
		return *createCustomFormat
	}

	fmt.Printf("  %2v | %-30v | %v | %v | %v |\n", "#", "Format", "Hash", "Encryption", "Block ID Length")
	fmt.Println(strings.Repeat("-", 76) + "+")
	for i, o := range cas.SupportedFormats {
		encryptionString := ""
		if o.IsEncrypted() {
			encryptionString = fmt.Sprintf("%d-bit", o.EncryptionKeySizeBits())
		}
		fmt.Printf("  %2v | %-30v | %4v | %10v | %15v |\n", i+1, o.Name, o.HashSizeBits(), encryptionString, o.BlockIDLength())
	}
	fmt.Println(strings.Repeat("-", 76) + "+")

	fmt.Printf("Select format (1-%d): ", len(cas.SupportedFormats))
	for {
		var number int

		if n, err := fmt.Scanf("%d\n", &number); n == 1 && err == nil && number >= 1 && number <= len(cas.SupportedFormats) {
			fmt.Printf("You selected '%v'\n", cas.SupportedFormats[number-1].Name)
			return cas.SupportedFormats[number-1].Name
		}

		fmt.Printf("Invalid selection. Select format (1-%d): ", len(cas.SupportedFormats))
	}
}
