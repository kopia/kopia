package vault

import (
	"crypto/rand"
	"fmt"
	"io"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/repo"
)

const (
	minUniqueIDLength = 32
)

// Format describes the format of a Vault.
// Contents of this structure are serialized in plain text in the Vault storage.
type Format struct {
	Version    string `json:"version"`
	UniqueID   []byte `json:"uniqueID"`
	Encryption string `json:"encryption"`
	Checksum   string `json:"checksum"`
}

func (f *Format) ensureUniqueID() error {
	if f.UniqueID == nil {
		f.UniqueID = make([]byte, minUniqueIDLength)
		if _, err := io.ReadFull(rand.Reader, f.UniqueID); err != nil {
			return err
		}
	}

	if len(f.UniqueID) < minUniqueIDLength {
		return fmt.Errorf("UniqueID too short, must be at least %v bytes", minUniqueIDLength)
	}

	return nil
}

type RepositoryConfig struct {
	Connection blob.ConnectionInfo `json:"connection"`
	Format     *repo.Format        `json:"format"`
}
