package vault

import (
	"net/url"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/cas"
)

type Vault interface {
}

type vault struct {
	storage blob.Storage
	repo    cas.Repository
}

func Open(vaultPath string, creds Credentials) (Vault, error) {
	var v vault
	var err error

	v.storage, err = openStorage(vaultPath)
	if err != nil {
		return nil, err
	}

	return &v, nil
}

func Create(vaultPath string, repositoryPath string, creds Credentials) (Vault, error) {
	var v vault
	return &v, nil
}

func openStorage(vaultPath string) (blob.Storage, error) {
	u, err := url.Parse(vaultPath)
	if err != nil {
		return nil, err
	}

	return blob.NewStorageFromURL(u)
}
