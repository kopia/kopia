package content

// Stats exposes statistics about content operation.
type Stats struct {
	// Keep int64 fields first to ensure they get aligned to at least 64-bit boundaries
	// which is required for atomic access on ARM and x86-32.
	ReadBytes      int64 `json:"readBytes,omitempty"`
	WrittenBytes   int64 `json:"writtenBytes,omitempty"`
	DecryptedBytes int64 `json:"decryptedBytes,omitempty"`
	EncryptedBytes int64 `json:"encryptedBytes,omitempty"`
	HashedBytes    int64 `json:"hashedBytes,omitempty"`

	ReadContents    int32 `json:"readContents,omitempty"`
	WrittenContents int32 `json:"writtenContents,omitempty"`
	HashedContents  int32 `json:"hashedContents,omitempty"`
	InvalidContents int32 `json:"invalidContents,omitempty"`
	ValidContents   int32 `json:"validContents,omitempty"`
}

// Reset clears all repository statistics.
func (s *Stats) Reset() {
	*s = Stats{}
}
