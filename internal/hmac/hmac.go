// Package hmac contains utilities for dealing with HMAC checksums.
package hmac

import (
	"crypto/hmac"
	"crypto/sha256"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
)

// Append computes HMAC-SHA256 checksum for a given block of bytes and appends it.
func Append(input gather.Bytes, secret []byte, output *gather.WriteBuffer) {
	h := hmac.New(sha256.New, secret)

	input.WriteTo(output) //nolint:errcheck
	input.WriteTo(h)      //nolint:errcheck

	var hash [sha256.Size]byte

	output.Write(h.Sum(hash[:0])) //nolint:errcheck
}

// VerifyAndStrip verifies that given block of bytes has correct HMAC-SHA256 checksum and strips it.
func VerifyAndStrip(input gather.Bytes, secret []byte, output *gather.WriteBuffer) error {
	if input.Length() < sha256.Size {
		return errors.New("invalid data - too short")
	}

	p := input.Length() - sha256.Size

	h := hmac.New(sha256.New, secret)
	r := input.Reader()

	if _, err := io.CopyN(io.MultiWriter(h, output), r, int64(p)); err != nil {
		return errors.Wrap(err, "error hashing")
	}

	var sigBuf, actualSignature [sha256.Size]byte
	validSignature := h.Sum(sigBuf[:0])

	n, err := r.Read(actualSignature[:])
	if err != nil || n != sha256.Size {
		return errors.Wrap(err, "error reading signature")
	}

	if hmac.Equal(validSignature, actualSignature[:]) {
		return nil
	}

	return errors.New("invalid data - corrupted")
}
