package content

import "crypto/hmac"
import "crypto/sha256"
import "errors"

func appendHMAC(data, secret []byte) []byte {
	h := hmac.New(sha256.New, secret)
	h.Write(data) // nolint:errcheck
	return h.Sum(data)
}

func verifyAndStripHMAC(b, secret []byte) ([]byte, error) {
	if len(b) < sha256.Size {
		return nil, errors.New("invalid data - too short")
	}

	p := len(b) - sha256.Size
	data := b[0:p]
	signature := b[p:]

	h := hmac.New(sha256.New, secret)
	h.Write(data) // nolint:errcheck
	validSignature := h.Sum(nil)
	if len(signature) != len(validSignature) {
		return nil, errors.New("invalid signature length")
	}
	if hmac.Equal(validSignature, signature) {
		return data, nil
	}

	return nil, errors.New("invalid data - corrupted")
}
