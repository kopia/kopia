package repo

import "github.com/kopia/kopia/blob"
import "encoding/hex"
import "fmt"

// Repository represents storage where both content-addressable and user-addressable data is kept.
type Repository struct {
	*ObjectManager
	*MetadataManager
	Storage blob.Storage

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

	MaxPackFileLength      int
	MaxPackedContentLength int
}

// Stats returns repository-wide statistics.
func (r *Repository) Stats() Stats {
	return r.ObjectManager.stats
}

// Status returns a snapshot of repository-wide statistics plus some general information about repository configuration.
func (r *Repository) Status() StatusInfo {
	s := StatusInfo{
		Stats: r.ObjectManager.stats,

		MetadataManagerVersion:      r.MetadataManager.format.Version,
		UniqueID:                    hex.EncodeToString(r.MetadataManager.format.UniqueID),
		MetadataEncryptionAlgorithm: r.MetadataManager.format.EncryptionAlgorithm,
		KeyDerivationAlgorithm:      r.MetadataManager.format.KeyDerivationAlgorithm,

		ObjectManagerVersion:   fmt.Sprintf("%v", r.ObjectManager.format.Version),
		ObjectFormat:           r.ObjectManager.format.ObjectFormat,
		Splitter:               r.ObjectManager.format.Splitter,
		MaxInlineContentLength: r.ObjectManager.format.MaxInlineContentLength,
		MinBlockSize:           r.ObjectManager.format.MinBlockSize,
		AvgBlockSize:           r.ObjectManager.format.AvgBlockSize,
		MaxBlockSize:           r.ObjectManager.format.MaxBlockSize,

		MaxPackFileLength:      r.ObjectManager.format.MaxPackFileLength,
		MaxPackedContentLength: r.ObjectManager.format.MaxPackedContentLength,
	}

	if s.Splitter == "" {
		s.Splitter = "FIXED"
	}

	return s
}

// Close closes the repository and releases all resources.
func (r *Repository) Close() error {
	if err := r.ObjectManager.Close(); err != nil {
		return err
	}
	if err := r.Storage.Close(); err != nil {
		return err
	}
	return nil
}

// Flush waits for all in-flight writes to complete.
func (r *Repository) Flush() error {
	r.ObjectManager.writeBack.flush()
	return nil
}

// ResetStats resets all repository-wide statistics to zero values.
func (r *Repository) ResetStats() {
	r.ObjectManager.stats = Stats{}
}
