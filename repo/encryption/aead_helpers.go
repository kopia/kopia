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

	var tmp gather.WriteBuffer
	defer tmp.Close()

	buf := tmp.MakeContiguous(resultLen)
	nonce, rest := buf[0:a.NonceSize()], buf[a.NonceSize():a.NonceSize()]

	n, err := rand.Read(nonce)
	if err != nil {
		return errors.Wrap(err, "unable to initialize nonce")
	}

	if n != a.NonceSize() {
		return errors.Errorf("did not read exactly %v bytes, got %v", a.NonceSize(), n)
	}

	a.Seal(rest, nonce, plaintext.ToByteSlice(), contentID)
	output.Append(buf)

	return nil
}

// aeadOpenPrefixedWithNonce opens AEAD-protected content, assuming first bytes are the nonce.
func aeadOpenPrefixedWithNonce(a cipher.AEAD, ciphertext gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	if ciphertext.Length() < a.NonceSize()+a.Overhead() {
		return errors.Errorf("ciphertext too short: %v", ciphertext.Length())
	}

	input := ciphertext.ToByteSlice()
	outbuf := output.MakeContiguous(ciphertext.Length() - a.NonceSize() - a.Overhead())

	if _, err := a.Open(outbuf[:0], input[0:a.NonceSize()], input[a.NonceSize():], contentID); err != nil {
		return errors.Errorf("unable to decrypt content")
	}

	return nil
}
