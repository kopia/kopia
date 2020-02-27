package encryption

import (
	"github.com/pkg/errors"
	"golang.org/x/crypto/salsa20"

	"github.com/kopia/kopia/internal/hmac"
)

const (
	purposeEncryptionKey = "encryption"
	purposeHMACSecret    = "hmac"
	hmacLength           = 32
	salsaKeyLength       = 32
)

type salsaEncryptor struct {
	nonceSize  int
	key        *[32]byte
	hmacSecret []byte
}

func (s salsaEncryptor) Decrypt(input, contentID []byte) ([]byte, error) {
	if s.hmacSecret != nil {
		var err error

		input, err = hmac.VerifyAndStrip(input, s.hmacSecret)
		if err != nil {
			return nil, errors.Wrap(err, "hmac.VerifyAndStrip")
		}
	}

	return s.encryptDecrypt(input, contentID)
}

func (s salsaEncryptor) Encrypt(input, contentID []byte) ([]byte, error) {
	v, err := s.encryptDecrypt(input, contentID)
	if err != nil {
		return nil, errors.Wrap(err, "decrypt")
	}

	if s.hmacSecret == nil {
		return v, nil
	}

	return hmac.Append(v, s.hmacSecret), nil
}

func (s salsaEncryptor) IsAuthenticated() bool {
	return s.hmacSecret != nil
}

func (s salsaEncryptor) encryptDecrypt(input, contentID []byte) ([]byte, error) {
	if len(contentID) < s.nonceSize {
		return nil, errors.Errorf("hash too short, expected >=%v bytes, got %v", s.nonceSize, len(contentID))
	}

	result := make([]byte, len(input))
	nonce := contentID[0:s.nonceSize]
	salsa20.XORKeyStream(result, input, nonce, s.key)

	return result, nil
}

func init() {
	Register("SALSA20", "SALSA20 using shared key and 64-bit nonce", true, func(p Parameters) (Encryptor, error) {
		var k [salsaKeyLength]byte
		copy(k[:], p.GetMasterKey()[0:salsaKeyLength])
		return salsaEncryptor{8, &k, nil}, nil
	})

	Register("SALSA20-HMAC", "SALSA20 with HMAC-SHA256 using shared key and 64-bit nonce", true, func(p Parameters) (Encryptor, error) {
		encryptionKey := deriveKey(p, []byte(purposeEncryptionKey), salsaKeyLength)
		hmacSecret := deriveKey(p, []byte(purposeHMACSecret), hmacLength)

		var k [salsaKeyLength]byte
		copy(k[:], encryptionKey)
		return salsaEncryptor{8, &k, hmacSecret}, nil
	})
}
