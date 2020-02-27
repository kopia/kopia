package encryption

import (
	"crypto/aes"
	"crypto/cipher"

	"github.com/pkg/errors"
)

// ctrEncryptor implements encrypted format which uses CTR mode of a content cipher with nonce==IV.
type ctrEncryptor struct {
	createCipher func() (cipher.Block, error)
}

func (fi ctrEncryptor) Encrypt(plainText, contentID []byte) ([]byte, error) {
	return symmetricEncrypt(fi.createCipher, contentID, plainText)
}

func (fi ctrEncryptor) Decrypt(cipherText, contentID []byte) ([]byte, error) {
	return symmetricEncrypt(fi.createCipher, contentID, cipherText)
}

func (fi ctrEncryptor) IsAuthenticated() bool {
	return false
}

func symmetricEncrypt(createCipher func() (cipher.Block, error), iv, b []byte) ([]byte, error) {
	blockCipher, err := createCipher()
	if err != nil {
		return nil, err
	}

	if len(iv) < blockCipher.BlockSize() {
		return nil, errors.Errorf("IV too short: %v expected >= %v", len(iv), blockCipher.BlockSize())
	}

	ctr := cipher.NewCTR(blockCipher, iv[0:blockCipher.BlockSize()])
	result := make([]byte, len(b))
	ctr.XORKeyStream(result, b)

	return result, nil
}

func adjustKey(masterKey []byte, desiredKeySize int) ([]byte, error) {
	if len(masterKey) == desiredKeySize {
		return masterKey, nil
	}

	if desiredKeySize < len(masterKey) {
		return masterKey[0:desiredKeySize], nil
	}

	return nil, errors.Errorf("required key too long %v, but only have %v", desiredKeySize, len(masterKey))
}

// newCTREncryptorFactory returns new EncryptorFactory that uses CTR with symmetric encryption (such as AES) and a given key size.
func newCTREncryptorFactory(keySize int, createCipherWithKey func(key []byte) (cipher.Block, error)) EncryptorFactory {
	return func(o Parameters) (Encryptor, error) {
		key, err := adjustKey(o.GetMasterKey(), keySize)
		if err != nil {
			return nil, errors.Wrap(err, "unable to get encryption key")
		}

		return ctrEncryptor{
			createCipher: func() (cipher.Block, error) {
				return createCipherWithKey(key)
			},
		}, nil
	}
}

func init() {
	Register("AES-128-CTR", "AES-128 in CTR mode", false, newCTREncryptorFactory(16, aes.NewCipher)) //nolint:gomnd
	Register("AES-192-CTR", "AES-192 in CTR mode", false, newCTREncryptorFactory(24, aes.NewCipher)) //nolint:gomnd
	Register("AES-256-CTR", "AES-256 in CTR mode", false, newCTREncryptorFactory(32, aes.NewCipher)) //nolint:gomnd
}
