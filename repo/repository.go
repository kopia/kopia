package repo

import "github.com/kopia/kopia/blob"
import "encoding/hex"
import "fmt"

// Repository represents storage where both content-addressable and user-addressable data is kept.
type Repository struct {
	Objects  *ObjectManager
	Metadata *MetadataManager
	Storage  blob.Storage

	ConfigFile     string
	CacheDirectory string
}

// StatusInfo stores a snapshot of repository-wide statistics plus some general information about repository configuration.
type StatusInfo struct {
	Stats

	MetadataManagerVersion      string
	MetadataEncryptionAlgorithm string
	UniqueID                    string
	KeyDerivationAlgorithm      string

	ObjectManagerVersion   string
	ObjectFormat           string
	MaxInlineContentLength int
	Splitter               string
	MinBlockSize           int
	AvgBlockSize           int
	MaxBlockSize           int

	MaxPackedContentLength int
}

// Stats returns repository-wide statistics.
func (r *Repository) Stats() Stats {
	return r.Objects.stats
}

// Status returns a snapshot of repository-wide statistics plus some general information about repository configuration.
func (r *Repository) Status() StatusInfo {
	s := StatusInfo{
		Stats: r.Objects.stats,

		MetadataManagerVersion:      r.Metadata.format.Version,
		UniqueID:                    hex.EncodeToString(r.Metadata.format.UniqueID),
		MetadataEncryptionAlgorithm: r.Metadata.format.EncryptionAlgorithm,
		KeyDerivationAlgorithm:      r.Metadata.format.KeyDerivationAlgorithm,

		ObjectManagerVersion: fmt.Sprintf("%v", r.Objects.format.Version),
		ObjectFormat:         r.Objects.format.ObjectFormat,
		Splitter:             r.Objects.format.Splitter,
		MinBlockSize:         r.Objects.format.MinBlockSize,
		AvgBlockSize:         r.Objects.format.AvgBlockSize,
		MaxBlockSize:         r.Objects.format.MaxBlockSize,

		MaxPackedContentLength: r.Objects.format.MaxPackedContentLength,
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

// ResetStats resets all repository-wide statistics to zero values.
func (r *Repository) ResetStats() {
	r.Objects.stats = Stats{}
}
