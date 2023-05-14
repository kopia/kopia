package azure

import (
	"encoding/json"
	"io"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

// ConfigName is the name of the storage config file in a Az storage account.
const ConfigName = ".storageconfig"

// PrefixAndStorageClass defines the storage class to use for a particular blob ID prefix.
type PrefixAndStorageClass struct {
	Prefix       blob.ID `json:"prefix"`
	StorageClass string  `json:"storageClass"`
}

// StorageConfig contains a collection of PrefixAndStorageClass.
type StorageConfig struct {
	BlobOptions []PrefixAndStorageClass `json:"blobOptions"`
}

// Load loads the StorageConfig from the provided reader.
func (p *StorageConfig) Load(r io.Reader) error {
	return errors.Wrap(json.NewDecoder(r).Decode(p), "error parsing JSON")
}

// Save saves the parameters to the provided writer.
func (p *StorageConfig) Save(w io.Writer) error {
	return errors.Wrap(json.NewEncoder(w).Encode(p), "error writing JSON")
}

// GetStorageClassForAzureBlobID return a StorageClass string for a particular blob ID.
func (p *StorageConfig) GetStorageClassForAzureBlobID(id blob.ID) string {
	for _, o := range p.BlobOptions {
		if strings.HasPrefix(string(id), string(o.Prefix)) {
			return o.StorageClass
		}
	}

	return ""
}
