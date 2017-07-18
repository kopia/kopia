package auth

// Credentials encapsulates credentials used to encrypt a Vault.
type Credentials interface {
	GetMasterKey(f Options) ([]byte, error)
}
