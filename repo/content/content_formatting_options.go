package content

import (
	"crypto/sha256"
	"io"

	"golang.org/x/crypto/hkdf"
)

// FormattingOptions describes the rules for formatting contents in repository.
type FormattingOptions struct {
	Version     int    `json:"version,omitempty"`     // version number, must be "1"
	Hash        string `json:"hash,omitempty"`        // identifier of the hash algorithm used
	Encryption  string `json:"encryption,omitempty"`  // identifier of the encryption algorithm used
	HMACSecret  []byte `json:"secret,omitempty"`      // HMAC secret used to generate encryption keys
	MasterKey   []byte `json:"masterKey,omitempty"`   // master encryption key (SIV-mode encryption only)
	MaxPackSize int    `json:"maxPackSize,omitempty"` // maximum size of a pack object
}

// DeriveKey uses HKDF to derive a key of a given length and a given purpose.
func (o FormattingOptions) DeriveKey(purpose []byte, length int) []byte {
	key := make([]byte, length)
	k := hkdf.New(sha256.New, o.MasterKey, purpose, nil)
	io.ReadFull(k, key) //nolint:errcheck
	return key

}
