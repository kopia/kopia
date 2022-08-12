package ecc

// Options represents the configuration for all ECC algorithms.
type Options struct {
	// Algorithm name to be used. Leave empty to disable error correction.
	Algorithm string `json:"algorithm,omitempty"`

	// OverheadPercent is how much more space can be used for ECC, in percentage.
	// Between 0 and 100. 0 means disable ECC.
	OverheadPercent int `json:"overheadPercent,omitempty"`

	// MaxShardSize represents the max shard size before splitting in blocks.
	// Use 0 to compute based on file size.
	MaxShardSize int `json:"maxShardSize,omitempty"`

	// Only set to true during benchmark tests
	DeleteFirstShardForTests bool
}

// DefaultAlgorithm is the name of the default ecc algorithm.
const DefaultAlgorithm = AlgorithmReedSolomonWithCrc32
