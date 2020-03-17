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

func (fi ctrEncryptor) Encrypt(output, plainText, contentID []byte) ([]byte, error) {
	return symmetricEncrypt(output, fi.createCipher, contentID, plainText)
}

func (fi ctrEncryptor) Decrypt(output, cipherText, contentID []byte) ([]byte, error) {
	return symmetricEncrypt(output, fi.createCipher, contentID, cipherText)
}

func (fi ctrEncryptor) IsAuthenticated() bool {
	return false
}

func (fi ctrEncryptor) IsDeprecated() bool {
	return true
}

func (fi ctrEncryptor) MaxOverhead() int {
	return 0
}

func symmetricEncrypt(output []byte, createCipher func() (cipher.Block, error), iv, b []byte) ([]byte, error) {
	blockCipher, err := createCipher()
	if err != nil {
		return nil, err
	}

	if len(iv) < blockCipher.BlockSize() {
		return nil, errors.Errorf("IV too short: %v expected >= %v", len(iv), blockCipher.BlockSize())
	}

	ctr := cipher.NewCTR(blockCipher, iv[0:blockCipher.BlockSize()])

	result, out := sliceForAppend(output, len(b))
	ctr.XORKeyStream(out, b)

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
	Register("AES-128-CTR", "DEPRECATED: AES-128 in CTR mode", true, newCTREncryptorFactory(16, aes.NewCipher)) //nolint:gomnd
	Register("AES-192-CTR", "DEPRECATED: AES-192 in CTR mode", true, newCTREncryptorFactory(24, aes.NewCipher)) //nolint:gomnd
	Register("AES-256-CTR", "DEPRECATED: AES-256 in CTR mode", true, newCTREncryptorFactory(32, aes.NewCipher)) //nolint:gomnd
}
