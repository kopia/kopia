package encryption

import (
	"crypto/cipher"
	"crypto/rand"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
)

// aeadSealWithRandomNonce returns AEAD-sealed content prepended with random nonce.
func aeadSealWithRandomNonce(a cipher.AEAD, plaintext gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	resultLen := plaintext.Length() + a.NonceSize() + a.Overhead()

	// allocate a single, contiguous slice that will be use as temporary input buffer
	// and also the output buffer for cipher.AEAD.Seal().

	// The buffer layout is:

	// input:  [nonce][plaintext][..unused..]
	// output: [nonce][ciphertext + overhead]
	var tmp gather.WriteBuffer
	defer tmp.Close()

	buf := tmp.MakeContiguous(resultLen)
	nonce, rest := buf[0:a.NonceSize()], buf[a.NonceSize():a.NonceSize()]

	input := plaintext.AppendToSlice(rest[:0])

	n, err := rand.Read(nonce)
	if err != nil {
		return errors.Wrap(err, "unable to initialize nonce")
	}

	if n != a.NonceSize() {
		return errors.Errorf("did not read exactly %v bytes, got %v", a.NonceSize(), n)
	}

	a.Seal(input[:0], nonce, input, contentID)
	output.Append(buf)

	return nil
}

// aeadOpenPrefixedWithNonce opens AEAD-protected content, assuming first bytes are the nonce.
func aeadOpenPrefixedWithNonce(a cipher.AEAD, ciphertext gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	if ciphertext.Length() < a.NonceSize()+a.Overhead() {
		return errors.Errorf("ciphertext too short: %v", ciphertext.Length())
	}

	// allocate a single, contiguous slice that will be use as temporary input buffer
	// and also the output buffer for cipher.AEAD.Open().

	// The buffer layout is:

	// input:  [nonce][ciphertext + overhead]
	// output: [nonce][plaintext]
	var tmp gather.WriteBuffer
	defer tmp.Close()

	buf := tmp.MakeContiguous(ciphertext.Length())
	buf = ciphertext.AppendToSlice(buf[:0])

	nonce, input := buf[0:a.NonceSize()], buf[a.NonceSize():]

	result, err := a.Open(input[:0], nonce, input, contentID)
	if err != nil {
		return errors.Errorf("unable to decrypt content: %v", err)
	}

	output.Append(result)

	return nil
}
