package main

import (
	"fmt"

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

	return f, nil
}

func openStorageAndEnsureEmpty(url string) (blob.Storage, error) {
	s, err := blob.NewStorageFromURL(url)
	if err != nil {
		return nil, err
	}
	ch := s.ListBlocks("")
	_, hasData := <-ch

	if hasData {
		return nil, fmt.Errorf("found existing data in %v", url)
	}

	return s, nil

}

func runCreateCommand(context *kingpin.ParseContext) error {
	vaultStorage, err := openStorageAndEnsureEmpty(*vaultPath)
	if err != nil {
		return fmt.Errorf("unable to get vault storage: %v", err)
		return err
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
		Storage:    repositoryStorage.Configuration(),
		Repository: repoFormat,
	})

	if *createCommandOnly {
		fmt.Println("Created vault:", *vaultPath)
		return nil
	}

	persistVaultConfig(v)

	fmt.Println("Created and connected to vault:", *vaultPath)

	return err
}
