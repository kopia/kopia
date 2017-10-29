package repo

import (
	"encoding/hex"
	"fmt"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/storage"
)

// Repository represents storage where both content-addressable and user-addressable data is kept.
type Repository struct {
	Blocks         *block.Manager
	Objects        *object.Manager
	Metadata       *MetadataManager
	Storage        storage.Storage
	KeyManager     *auth.KeyManager
	metadataFormat *config.MetadataFormat

	ConfigFile     string
	CacheDirectory string
}

// StatusInfo stores a snapshot of repository-wide statistics plus some general information about repository configuration.
type StatusInfo struct {
	block.Stats

	MetadataManagerVersion      string
	MetadataEncryptionAlgorithm string
	UniqueID                    string
	KeyDerivationAlgorithm      string

	ObjectManagerVersion string
	BlockFormat          string
	Splitter             string
	MinBlockSize         int
	AvgBlockSize         int
	MaxBlockSize         int

	MaxPackedContentLength int
}

// Status returns a snapshot of repository-wide statistics plus some general information about repository configuration.
func (r *Repository) Status() StatusInfo {
	s := StatusInfo{
		Stats: r.Blocks.Stats(),

		MetadataManagerVersion:      r.metadataFormat.Version,
		UniqueID:                    hex.EncodeToString(r.metadataFormat.UniqueID),
		MetadataEncryptionAlgorithm: r.metadataFormat.EncryptionAlgorithm,
		KeyDerivationAlgorithm:      r.metadataFormat.KeyDerivationAlgorithm,

		ObjectManagerVersion: fmt.Sprintf("%v", r.Objects.Format.Version),
		BlockFormat:          r.Objects.Format.BlockFormat,
		Splitter:             r.Objects.Format.Splitter,
		MinBlockSize:         r.Objects.Format.MinBlockSize,
		AvgBlockSize:         r.Objects.Format.AvgBlockSize,
		MaxBlockSize:         r.Objects.Format.MaxBlockSize,

		MaxPackedContentLength: r.Objects.Format.MaxPackedContentLength,
	}

	if s.Splitter == "" {
		s.Splitter = "FIXED"
	}

	return s
}

// Close closes the repository and releases all resources.
func (r *Repository) Close() error {
	if err := r.Objects.Close(); err != nil {
		return err
	}
	if err := r.Storage.Close(); err != nil {
		return err
	}
	return nil
}

// Flush waits for all in-flight writes to complete.
func (r *Repository) Flush() error {
	return r.Objects.Flush()
}
