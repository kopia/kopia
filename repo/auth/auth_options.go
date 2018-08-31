package auth

// SecurityOptions represents options related to authentication (e.g. key derivation)
type SecurityOptions struct {
	UniqueID               []byte `json:"uniqueID"`
	KeyDerivationAlgorithm string `json:"keyAlgo"`
}
