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
	"github.com/kopia/kopia/repo"

	"golang.org/x/crypto/hkdf"
	"golang.org/x/crypto/pbkdf2"
)

const (
	formatBlock           = "format"
	checksumBlock         = "checksum"
	repositoryConfigBlock = "repo"

	// MinPasswordLength is the minimum allowed length of a password in charcters.
	MinPasswordLength = 12

	// MinMasterKeyLength is the minimum allowed length of a master key, in bytes.
	MinMasterKeyLength = 16
)

var (
	purposeAESKey         = []byte("AES")
	purposeChecksumSecret = []byte("CHECKSUM")
)

// Vault is a secure storage for the secrets.
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

	return v.Storage.PutBlock(name, ioutil.NopCloser(bytes.NewBuffer(cipherText)), blob.PutOptions{
		Overwrite: true,
	})
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
		return nil, fmt.Errorf("cannot read encrypted block: incorrect checksum")
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
	case "aes-128":
		k := make([]byte, 16)
		v.deriveKey(purposeAESKey, k)
		return aes.NewCipher(k)
	case "aes-192":
		k := make([]byte, 24)
		v.deriveKey(purposeAESKey, k)
		return aes.NewCipher(k)
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

func (v *Vault) RepositoryConfig() (*RepositoryConfig, error) {
	var rc RepositoryConfig

	b, err := v.readEncryptedBlock(repositoryConfigBlock)
	if err != nil {
		return nil, fmt.Errorf("unable to read repository: %v", err)
	}

	err = json.Unmarshal(b, &rc)
	if err != nil {
		return nil, err
	}

	return &rc, nil
}

func (v *Vault) OpenRepository() (repo.Repository, error) {
	rc, err := v.RepositoryConfig()
	if err != nil {
		return nil, err
	}

	storage, err := blob.NewStorage(rc.Storage)
	if err != nil {
		return nil, fmt.Errorf("unable to open repository: %v", err)
	}

	return repo.NewRepository(storage, rc.Format)
}

func (v *Vault) Get(id string, content interface{}) error {
	j, err := v.readEncryptedBlock(id)
	if err != nil {
		return nil
	}

	return json.Unmarshal(j, content)
}

func (v *Vault) Put(id string, content interface{}) error {
	j, err := json.Marshal(content)
	if err != nil {
		return err
	}
	return v.writeEncryptedBlock(id, j)
}

func (v *Vault) List(prefix string) ([]string, error) {
	var result []string

	for b := range v.Storage.ListBlocks(prefix) {
		if b.Error != nil {
			return result, b.Error
		}
		result = append(result, b.BlockID)
	}
	return result, nil
}

// CreateWithPassword creates a password-protected Vault in the specified storage.
func CreateWithPassword(storage blob.Storage, format *Format, password string) (*Vault, error) {
	if err := format.ensureUniqueID(); err != nil {
		return nil, err
	}

	if len(password) < MinPasswordLength {
		return nil, fmt.Errorf("password too short, must be at least %v characters, got %v", MinPasswordLength, len(password))
	}
	masterKey := pbkdf2.Key([]byte(password), format.UniqueID, pbkdf2Rounds, masterKeySize, sha256.New)
	return CreateWithMasterKey(storage, format, masterKey)
}

// CreateWithMasterKey creates a master key-protected Vault in the specified storage.
func CreateWithMasterKey(storage blob.Storage, format *Format, masterKey []byte) (*Vault, error) {
	if len(masterKey) < MinMasterKeyLength {
		return nil, fmt.Errorf("key too short, must be at least %v bytes, got %v", MinMasterKeyLength, len(masterKey))
	}

	v := Vault{
		Storage:   storage,
		MasterKey: masterKey,
		Format:    *format,
	}
	v.Format.Version = "1"
	if err := v.Format.ensureUniqueID(); err != nil {
		return nil, err
	}

	formatBytes, err := json.Marshal(&v.Format)
	if err != nil {
		return nil, err
	}

	storage.PutBlock(formatBlock, ioutil.NopCloser(bytes.NewBuffer(formatBytes)), blob.PutOptions{
		Overwrite: true,
	})

	vv := make([]byte, 512)
	if _, err := io.ReadFull(rand.Reader, vv); err != nil {
		return nil, err
	}

	err = v.writeEncryptedBlock(checksumBlock, vv)
	if err != nil {
		return nil, err
	}

	return OpenWithMasterKey(storage, masterKey)
}

// OpenWithPassword opens a password-protected vault.
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

// OpenWithMasterKey opens a master key-protected vault.
func OpenWithMasterKey(storage blob.Storage, masterKey []byte) (*Vault, error) {
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
