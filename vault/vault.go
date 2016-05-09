package vault

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/ioutil"

	"github.com/kopia/kopia/blob"

	"golang.org/x/crypto/hkdf"
	"golang.org/x/crypto/pbkdf2"
)

const (
	formatBlock           = "format"
	checksumBlock         = "checksum"
	repositoryConfigBlock = "repo"
	minPasswordLength     = 12
)

var (
	purposeAESKey         = []byte("AES")
	purposeChecksumSecret = []byte("CHECKSUM")
)

type Vault struct {
	Storage   blob.Storage
	MasterKey []byte
	Format    Format
}

func (v *Vault) writeEncryptedBlock(name string, content []byte) error {
	blk, err := v.newCipher()
	if err != nil {
		return err
	}

	hash, err := v.newChecksum()
	if err != nil {
		return err
	}

	ivLength := blk.BlockSize()
	ivPlusContentLength := ivLength + len(content)
	cipherText := make([]byte, ivPlusContentLength+hash.Size())

	// Store IV at the beginning of ciphertext.
	iv := cipherText[0:ivLength]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return err
	}

	ctr := cipher.NewCTR(blk, iv)
	ctr.XORKeyStream(cipherText[ivLength:], content)
	hash.Write(cipherText[0:ivPlusContentLength])
	copy(cipherText[ivPlusContentLength:], hash.Sum(nil))

	return v.Storage.PutBlock(name, ioutil.NopCloser(bytes.NewBuffer(cipherText)), blob.PutOptions{})
}

func (v *Vault) readEncryptedBlock(name string) ([]byte, error) {
	cipherText, err := v.Storage.GetBlock(name)
	if err != nil {
		return nil, err
	}

	hash, err := v.newChecksum()
	if err != nil {
		return nil, err
	}

	p := len(cipherText) - hash.Size()
	hash.Write(cipherText[0:p])
	expectedChecksum := hash.Sum(nil)
	actualChecksum := cipherText[p:]
	if !hmac.Equal(expectedChecksum, actualChecksum) {
		return nil, fmt.Errorf("cannot read encrypted block.")
	}

	blk, err := v.newCipher()
	if err != nil {
		return nil, err
	}

	ivLength := blk.BlockSize()

	plainText := make([]byte, len(cipherText)-ivLength-hash.Size())
	iv := cipherText[0:blk.BlockSize()]

	ctr := cipher.NewCTR(blk, iv)
	ctr.XORKeyStream(plainText, cipherText[ivLength:len(cipherText)-hash.Size()])
	return plainText, nil
}

func (v *Vault) newChecksum() (hash.Hash, error) {
	switch v.Format.Checksum {
	case "hmac-sha-256":
		key := make([]byte, 32)
		v.deriveKey(purposeChecksumSecret, key)
		return hmac.New(sha256.New, key), nil

	default:
		return nil, fmt.Errorf("unsupported checksum format: %v", v.Format.Checksum)
	}

}

func (v *Vault) newCipher() (cipher.Block, error) {
	switch v.Format.Encryption {
	case "aes-256":
		k := make([]byte, 32)
		v.deriveKey(purposeAESKey, k)
		return aes.NewCipher(k)
	default:
		return nil, fmt.Errorf("unsupported encryption format: %v", v.Format.Encryption)
	}

}

func (v *Vault) deriveKey(purpose []byte, key []byte) error {
	k := hkdf.New(sha256.New, v.MasterKey, v.Format.UniqueID, purpose)
	_, err := io.ReadFull(k, key)
	return err
}

func (v *Vault) SetRepository(rc RepositoryConfig) error {
	b, err := json.Marshal(&rc)
	if err != nil {
		return err
	}

	return v.writeEncryptedBlock(repositoryConfigBlock, b)
}

func (v *Vault) Repository() (RepositoryConfig, error) {
	var rc RepositoryConfig

	b, err := v.readEncryptedBlock(repositoryConfigBlock)
	if err != nil {
		return rc, fmt.Errorf("unable to read repository: %v", err)
	}

	err = json.Unmarshal(b, &rc)
	return rc, err
}

func CreateWithPassword(storage blob.Storage, format *Format, password string) (*Vault, error) {
	if err := format.ensureUniqueID(); err != nil {
		return nil, err
	}

	if len(password) < minPasswordLength {
		return nil, fmt.Errorf("password too short, must be at least %v characters, got %v", minPasswordLength, len(password))
	}
	masterKey := pbkdf2.Key([]byte(password), format.UniqueID, pbkdf2Rounds, masterKeySize, sha256.New)
	return CreateWithKey(storage, format, masterKey)
}

func CreateWithKey(storage blob.Storage, format *Format, masterKey []byte) (*Vault, error) {
	ok, err := storage.BlockExists(formatBlock)
	if ok {
		return nil, fmt.Errorf("vault already exists")
	}
	if err != nil {
		return nil, err
	}

	formatBytes, err := json.Marshal(&format)
	if err != nil {
		return nil, err
	}

	storage.PutBlock(formatBlock, ioutil.NopCloser(bytes.NewBuffer(formatBytes)), blob.PutOptions{})

	v := Vault{
		Storage:   storage,
		MasterKey: masterKey,
		Format:    *format,
	}
	v.Format.Version = "1"
	if err := v.Format.ensureUniqueID(); err != nil {
		return nil, err
	}

	vv := make([]byte, 512)
	if _, err := io.ReadFull(rand.Reader, vv); err != nil {
		return nil, err
	}

	err = v.writeEncryptedBlock(checksumBlock, vv)
	if err != nil {
		return nil, err
	}

	return OpenWithKey(storage, masterKey)
}

func OpenWithPassword(storage blob.Storage, password string) (*Vault, error) {
	v := Vault{
		Storage: storage,
	}

	f, err := storage.GetBlock(formatBlock)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(f, &v.Format)
	if err != nil {
		return nil, err
	}

	v.MasterKey = pbkdf2.Key([]byte(password), v.Format.UniqueID, pbkdf2Rounds, masterKeySize, sha256.New)

	if _, err := v.readEncryptedBlock(checksumBlock); err != nil {
		return nil, err
	}

	return &v, nil
}

func OpenWithKey(storage blob.Storage, masterKey []byte) (*Vault, error) {
	v := Vault{
		Storage:   storage,
		MasterKey: masterKey,
	}

	f, err := storage.GetBlock(formatBlock)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(f, &v.Format)
	if err != nil {
		return nil, err
	}

	if _, err := v.readEncryptedBlock(checksumBlock); err != nil {
		return nil, err
	}

	return &v, nil
}
