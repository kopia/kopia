package repo

import (
	"encoding/json"
	"fmt"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/metadata"
	"github.com/kopia/kopia/storage"
)

const (
	formatBlockID           = "format"
	repositoryConfigBlockID = "repo"
)

type formatBlock struct {
	auth.SecurityOptions
	metadata.Format
}

// encryptedRepositoryConfig contains the configuration of repository that's persisted in encrypted format.
type encryptedRepositoryConfig struct {
	Format config.RepositoryObjectFormat `json:"format"`
}

func readFormaBlock(st storage.Storage) (*formatBlock, error) {
	f := &formatBlock{}

	b, err := st.GetBlock(metadata.MetadataBlockPrefix+"format", 0, -1)
	if err != nil {
		return nil, fmt.Errorf("unable to read format block: %v", err)
	}

	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("invalid format block: %v", err)
	}
	return f, err
}

func writeFormatBlock(st storage.Storage, f *formatBlock) error {
	formatBytes, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("unable to marshal format block: %v", err)
	}

	if err := st.PutBlock(metadata.MetadataBlockPrefix+formatBlockID, formatBytes); err != nil {
		return fmt.Errorf("unable to write format block: %v", err)
	}

	return nil
}

func readEncryptedConfig(mm *metadata.Manager) (*encryptedRepositoryConfig, error) {
	erc := &encryptedRepositoryConfig{}

	if err := mm.GetJSON(repositoryConfigBlockID, erc); err != nil {
		return nil, fmt.Errorf("unable to read repository configuration: %v", err)
	}

	return erc, nil
}

func writeEncryptedConfig(mm *metadata.Manager, erc *encryptedRepositoryConfig) error {
	if err := mm.PutJSON(repositoryConfigBlockID, erc); err != nil {
		return err
	}

	return nil
}
