// Package transform implements support for transformations over blob data
package transform

import "github.com/kopia/kopia/internal/gather"

// Transformer performs encryption and decryption of contents of data.
type Transformer interface {
	// ToRepository appends the transformed bytes corresponding to the given input to a given slice.
	// Must not clobber the input slice.
	ToRepository(input gather.Bytes, contentID []byte, output *gather.WriteBuffer) error

	// FromRepository appends the un-transformed bytes corresponding to the given input to a given slice.
	// Must not clobber the input slice.
	FromRepository(cipherText gather.Bytes, contentID []byte, output *gather.WriteBuffer) error

	// Overhead is the number of bytes of overhead added by Encrypt()
	Overhead() int
}
