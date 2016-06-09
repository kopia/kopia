package vault

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"strings"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/repo"

	"golang.org/x/crypto/hkdf"
)

const (
	formatBlock           = "format"
	checksumBlock         = "checksum"
	repositoryConfigBlock = "repo"

	storedObjectIDPrefix      = "v"
	storedObjectIDLengthBytes = 8
)

var (
	purposeAESKey         = []byte("AES")
	purposeChecksumSecret = []byte("CHECKSUM")
)

// Vault is a secure storage for the secrets.
type Vault struct {
	storage   blob.Storage
	masterKey []byte
	format    Format
}

func (v *Vault) writeEncryptedBlock(name string, content []byte) error {
	blk, err := v.newCipher()
	if err != nil {
		return err
	}

	if blk != nil {
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

		content = cipherText
	}

	return v.storage.PutBlock(name, blob.NewReader(bytes.NewBuffer(content)), blob.PutOptionsOverwrite)
}

func (v *Vault) readEncryptedBlock(name string) ([]byte, error) {
	content, err := v.storage.GetBlock(name)
	if err != nil {
		return nil, err
	}

	blk, err := v.newCipher()
	if err != nil {
		return nil, err
	}

	if blk != nil {

		hash, err := v.newChecksum()
		if err != nil {
			return nil, err
		}

		p := len(content) - hash.Size()
		hash.Write(content[0:p])
		expectedChecksum := hash.Sum(nil)
		actualChecksum := content[p:]
		if !hmac.Equal(expectedChecksum, actualChecksum) {
			return nil, fmt.Errorf("cannot read encrypted block: incorrect checksum")
		}

		ivLength := blk.BlockSize()

		plainText := make([]byte, len(content)-ivLength-hash.Size())
		iv := content[0:blk.BlockSize()]

		ctr := cipher.NewCTR(blk, iv)
		ctr.XORKeyStream(plainText, content[ivLength:len(content)-hash.Size()])

		content = plainText
	}

	return content, nil
}

func (v *Vault) newChecksum() (hash.Hash, error) {
	switch v.format.Checksum {
	case "hmac-sha-256":
		key := make([]byte, 32)
		v.deriveKey(purposeChecksumSecret, key)
		return hmac.New(sha256.New, key), nil

	default:
		return nil, fmt.Errorf("unsupported checksum format: %v", v.format.Checksum)
	}

}

func (v *Vault) newCipher() (cipher.Block, error) {
	switch v.format.Encryption {
	case "none":
		return nil, nil
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
		return nil, fmt.Errorf("unsupported encryption format: %v", v.format.Encryption)
	}

}

func (v *Vault) deriveKey(purpose []byte, key []byte) error {
	k := hkdf.New(sha256.New, v.masterKey, v.format.UniqueID, purpose)
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

func (v *Vault) GetRaw(id string) ([]byte, error) {
	return v.readEncryptedBlock(id)
}

// Get deserializes JSON data stored in the vault into the specified content structure.
func (v *Vault) Get(id string, content interface{}) error {
	j, err := v.readEncryptedBlock(id)
	if err != nil {
		return nil
	}

	return json.Unmarshal(j, content)
}

// Put stores the contents of an item stored in a vault with a given ID.
func (v *Vault) Put(id string, content interface{}) error {
	j, err := json.Marshal(content)
	if err != nil {
		return err
	}
	return v.writeEncryptedBlock(id, j)
}

func (v *Vault) List(prefix string) ([]string, error) {
	var result []string

	for b := range v.storage.ListBlocks(prefix) {
		if b.Error != nil {
			return result, b.Error
		}
		result = append(result, b.BlockID)
	}
	return result, nil
}

type vaultConfig struct {
	Storage blob.StorageConfiguration `json:"storage"`
	Key     []byte                    `json:"key,omitempty"`
}

// Token returns a persistent opaque string that encodes the configuration of vault storage
// and its credentials in a way that can be later used to open the vault.
func (v *Vault) Token() (string, error) {
	vc := vaultConfig{
		Storage: v.storage.Configuration(),
		Key:     v.masterKey,
	}

	b, err := json.Marshal(&vc)
	if err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(b), nil
}

type objectIDData struct {
	ObjectID string `json:"objectID"`
}

func (v *Vault) SaveObjectID(oid repo.ObjectID) (string, error) {
	h := hmac.New(sha256.New, v.format.UniqueID)
	h.Write([]byte(oid))
	sum := h.Sum(nil)
	for i := storedObjectIDLengthBytes; i < len(sum); i++ {
		sum[i%storedObjectIDLengthBytes] ^= sum[i]
	}
	sum = sum[0:storedObjectIDLengthBytes]
	key := storedObjectIDPrefix + hex.EncodeToString(sum)

	var d objectIDData
	d.ObjectID = string(oid)

	if err := v.Put(key, &d); err != nil {
		return "", err
	}

	return key, nil
}

func (v *Vault) ResolveObjectID(id string) (repo.ObjectID, error) {
	if !strings.HasPrefix(id, storedObjectIDPrefix) {
		return repo.ParseObjectID(id)
	}

	matches, err := v.List(id)
	if err != nil {
		return "", err
	}

	switch len(matches) {
	case 0:
		return "", fmt.Errorf("object not found: %v", id)
	case 1:
		var d objectIDData
		if err := v.Get(matches[0], &d); err != nil {
			return "", err
		}
		return repo.ParseObjectID(d.ObjectID)

	default:
		return "", fmt.Errorf("ambiguous object ID: %v", id)
	}
}

func (v *Vault) Remove(id string) error {
	return v.storage.DeleteBlock(id)
}

// Create creates a Vault in the specified storage.
func Create(storage blob.Storage, format *Format, creds Credentials) (*Vault, error) {
	if err := format.ensureUniqueID(); err != nil {
		return nil, err
	}

	v := Vault{
		storage: storage,
		format:  *format,
	}
	v.format.Version = "1"
	if err := v.format.ensureUniqueID(); err != nil {
		return nil, err
	}

	v.masterKey = creds.getMasterKey(v.format.UniqueID)

	formatBytes, err := json.Marshal(&v.format)
	if err != nil {
		return nil, err
	}

	storage.PutBlock(formatBlock, blob.NewReader(bytes.NewBuffer(formatBytes)), blob.PutOptionsOverwrite)

	vv := make([]byte, 512)
	if _, err := io.ReadFull(rand.Reader, vv); err != nil {
		return nil, err
	}

	err = v.writeEncryptedBlock(checksumBlock, vv)
	if err != nil {
		return nil, err
	}

	return Open(storage, creds)
}

// Open opens a vault.
func Open(storage blob.Storage, creds Credentials) (*Vault, error) {
	v := Vault{
		storage: storage,
	}

	f, err := storage.GetBlock(formatBlock)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(f, &v.format)
	if err != nil {
		return nil, err
	}

	v.masterKey = creds.getMasterKey(v.format.UniqueID)

	if _, err := v.readEncryptedBlock(checksumBlock); err != nil {
		return nil, err
	}

	return &v, nil
}

// OpenWithToken opens a vault with storage configuration and credentials in the specified token.
func OpenWithToken(token string) (*Vault, error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("invalid vault token")
	}

	var vc vaultConfig
	err = json.Unmarshal(b, &vc)
	if err != nil {
		return nil, fmt.Errorf("invalid vault token")
	}

	st, err := blob.NewStorage(vc.Storage)
	if err != nil {
		return nil, fmt.Errorf("cannot open vault storage: %v", err)
	}

	creds, err := MasterKey(vc.Key)
	if err != nil {
		return nil, fmt.Errorf("invalid vault token")
	}

	return Open(st, creds)
}
