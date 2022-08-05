package ecc

// Options must be anonymously embedded in sharded provider options.
type Options struct {
	// Algorithm name to be used. Leave empty to disable error correction.
	Algorithm string `json:"algorithm,omitempty"`
}

var DefaultAlgorithm = ReedSolomonCrc322pEccName
