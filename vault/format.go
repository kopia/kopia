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

func NewFormat() *Format {
	return &Format{
		Version:    "1",
		Encryption: "aes-256",
		Checksum:   "hmac-sha-256",
	}
}

type RepositoryConfig struct {
	Storage blob.StorageConfiguration `json:"storage"`
	Format  *repo.Format              `json:"repository"`
}
