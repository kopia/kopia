// Package hmac contains utilities for dealing with HMAC checksums.
package hmac

import (
	"crypto/hmac"
	"crypto/sha256"

	"github.com/pkg/errors"
)

// Append computes HMAC-SHA256 checksum for a given block of bytes and appends it.
func Append(data, secret []byte) []byte {
	h := hmac.New(sha256.New, secret)
	h.Write(data) // nolint:errcheck

	return h.Sum(data)
}

// VerifyAndStrip verifies that given block of bytes has correct HMAC-SHA256 checksum and strips it.
func VerifyAndStrip(b, secret []byte) ([]byte, error) {
	if len(b) < sha256.Size {
		return nil, errors.New("invalid data - too short")
	}

	p := len(b) - sha256.Size
	data := b[0:p]
	signature := b[p:]

	h := hmac.New(sha256.New, secret)
	h.Write(data) // nolint:errcheck

	var sigBuf [32]byte
	validSignature := h.Sum(sigBuf[:0])

	if len(signature) != len(validSignature) {
		return nil, errors.New("invalid signature length")
	}

	if hmac.Equal(validSignature, signature) {
		return data, nil
	}

	return nil, errors.New("invalid data - corrupted")
}
