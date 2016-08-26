package repo

// Stats exposes statistics about Repository operation.
type Stats struct {
	// Keep int64 fields first to ensure they get aligned to at least 64-bit boundaries
	// which is required for atomic access on ARM and x86-32.
	ReadBytes      int64 `json:"readBytes,omitempty"`
	WrittenBytes   int64 `json:"writtenBytes,omitempty"`
	DecryptedBytes int64 `json:"decryptedBytes,omitempty"`
	EncryptedBytes int64 `json:"encryptedBytes,omitempty"`
	HashedBytes    int64 `json:"hashedBytes,omitempty"`

	ReadBlocks    int32 `json:"readBlocks,omitempty"`
	WrittenBlocks int32 `json:"writtenBlocks,omitempty"`
	CheckedBlocks int32 `json:"checkedBlocks,omitempty"`
	HashedBlocks  int32 `json:"hashedBlocks,omitempty"`
	InvalidBlocks int32 `json:"invalidBlocks,omitempty"`
	PresentBlocks int32 `json:"presentBlocks,omitempty"`
	ValidBlocks   int32 `json:"validBlocks,omitempty"`
}
