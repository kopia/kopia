// Package auth encapsulates credentials to decrypt repository format block.
package auth

// Credentials encapsulates credentials used to derive master key for repository encryption.
type Credentials interface {
	GetMasterKey(f SecurityOptions) ([]byte, error)
}
