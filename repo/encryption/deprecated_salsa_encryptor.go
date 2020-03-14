package encryption

import (
	"crypto/sha256"

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

func (s salsaEncryptor) Decrypt(output, input, contentID []byte) ([]byte, error) {
	if s.hmacSecret != nil {
		var err error

		input, err = hmac.VerifyAndStrip(input, s.hmacSecret)
		if err != nil {
			return nil, errors.Wrap(err, "hmac.VerifyAndStrip")
		}
	}

	return s.encryptDecrypt(output, input, contentID)
}

func (s salsaEncryptor) Encrypt(output, input, contentID []byte) ([]byte, error) {
	v, err := s.encryptDecrypt(output, input, contentID)
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

func (s salsaEncryptor) MaxOverhead() int {
	if s.hmacSecret == nil {
		return 0
	}

	return sha256.Size
}

func (s salsaEncryptor) encryptDecrypt(output, input, contentID []byte) ([]byte, error) {
	if len(contentID) < s.nonceSize {
		return nil, errors.Errorf("hash too short, expected >=%v bytes, got %v", s.nonceSize, len(contentID))
	}

	result, out := sliceForAppend(output, len(input))
	nonce := contentID[0:s.nonceSize]
	salsa20.XORKeyStream(out, input, nonce, s.key)

	return result, nil
}

func (s salsaEncryptor) IsDeprecated() bool {
	return true
}

func init() {
	Register("SALSA20", "DEPRECATED: SALSA20 using shared key and 64-bit nonce", true, func(p Parameters) (Encryptor, error) {
		var k [salsaKeyLength]byte
		copy(k[:], p.GetMasterKey()[0:salsaKeyLength])
		return salsaEncryptor{8, &k, nil}, nil
	})

	Register("SALSA20-HMAC", "DEPRECATED: SALSA20 with HMAC-SHA256 using shared key and 64-bit nonce", true, func(p Parameters) (Encryptor, error) {
		encryptionKey, err := deriveKey(p, []byte(purposeEncryptionKey), salsaKeyLength)
		if err != nil {
			return nil, err
		}
		hmacSecret, err := deriveKey(p, []byte(purposeHMACSecret), hmacLength)
		if err != nil {
			return nil, err
		}

		var k [salsaKeyLength]byte
		copy(k[:], encryptionKey)
		return salsaEncryptor{8, &k, hmacSecret}, nil
	})
}
