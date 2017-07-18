package auth

// Options represents options related to authentication.
type Options struct {
	UniqueID               []byte `json:"uniqueID"`
	KeyDerivationAlgorithm string `json:"keyAlgo"`
}
