package s3

import (
	"encoding/json"
	"io"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

// ConfigName is the name of the hidden storage config file in a S3 bucket.
const ConfigName = ".storageconfig"

// PrefixAndStorageClass defines the storage class to use for a particular blob ID prefix.
type PrefixAndStorageClass struct {
	Prefix       blob.ID `json:"prefix"`
	StorageClass string  `json:"storageClass"`
}

// StorageConfig contains storage configuration optionally persisted in the storage itself.
type StorageConfig struct {
	BlobOptions []PrefixAndStorageClass `json:"blobOptions,omitempty"`
}

// Load loads the StorageConfig from the provided reader.
func (p *StorageConfig) Load(r io.Reader) error {
	return errors.Wrap(json.NewDecoder(r).Decode(p), "error parsing JSON")
}

// Save saves the parameters to the provided writer.
func (p *StorageConfig) Save(w io.Writer) error {
	return errors.Wrap(json.NewEncoder(w).Encode(p), "error writing JSON")
}

func (p *StorageConfig) getStorageClassForBlobID(id blob.ID) string {
	for _, o := range p.BlobOptions {
		if strings.HasPrefix(string(id), string(o.Prefix)) {
			return o.StorageClass
		}
	}

	return ""
}
