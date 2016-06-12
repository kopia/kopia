package vault

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/repo"

	"golang.org/x/crypto/hkdf"
)

const (
	formatBlockID           = "format"
	repositoryConfigBlockID = "repo"

	colocatedVaultItemPrefix = "VLT"
)

var (
	purposeAESKey         = []byte("AES")
	purposeChecksumSecret = []byte("CHECKSUM")
)

// ErrItemNotFound is an error returned when a vault item cannot be found.
var ErrItemNotFound = errors.New("item not found")

// Vault is a secure storage for the secrets.
type Vault struct {
	storage    blob.Storage
	masterKey  []byte
	format     Format
	itemPrefix string
	repoConfig repositoryConfig
}

type repositoryConfig struct {
	Connection *blob.ConnectionInfo `json:"connection"`
	Format     *repo.Format         `json:"format"`
}

// Storage returns the underlying blob storage that stores the repository.
func (v *Vault) Storage() blob.Storage {
	return v.storage
}

// Format returns the vault format.
func (v *Vault) Format() Format {
	return v.format
}

// Put saves the specified content in a vault under a specified name.
func (v *Vault) Put(itemID string, content []byte) error {
	if err := checkReservedName(itemID); err != nil {
		return err
	}

	return v.writeEncryptedBlock(itemID, content)
}

func (v *Vault) writeEncryptedBlock(itemID string, content []byte) error {
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

	return v.storage.PutBlock(
		v.itemPrefix+itemID,
		blob.NewReader(bytes.NewBuffer(content)),
		blob.PutOptionsOverwrite,
	)
}

func (v *Vault) readEncryptedBlock(itemID string) ([]byte, error) {
	content, err := v.storage.GetBlock(v.itemPrefix + itemID)
	if err != nil {
		if err == blob.ErrBlockNotFound {
			return nil, ErrItemNotFound
		}
		return nil, fmt.Errorf("unexpected error reading %v: %v", itemID, err)
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

// RepositoryFormat returns the format of the repository.
func (v *Vault) RepositoryFormat() *repo.Format {
	return v.repoConfig.Format
}

// OpenRepository connects to the repository the vault is associated with.
func (v *Vault) OpenRepository() (repo.Repository, error) {
	var storage blob.Storage
	var err error

	if v.repoConfig.Connection != nil {
		storage, err = blob.NewStorage(*v.repoConfig.Connection)
		if err != nil {
			return nil, fmt.Errorf("unable to open repository: %v", err)
		}
	} else {
		storage = v.storage
	}

	return repo.NewRepository(storage, v.repoConfig.Format)
}

// Get returns the contents of a specified vault item.
func (v *Vault) Get(itemID string) ([]byte, error) {
	if err := checkReservedName(itemID); err != nil {
		return nil, err
	}

	return v.readEncryptedBlock(itemID)
}

func (v *Vault) getJSON(itemID string, content interface{}) error {
	j, err := v.readEncryptedBlock(itemID)
	if err != nil {
		return err
	}

	return json.Unmarshal(j, content)
}

// Put stores the contents of an item stored in a vault with a given ID.
func (v *Vault) putJSON(id string, content interface{}) error {
	j, err := json.Marshal(content)
	if err != nil {
		return err
	}

	return v.writeEncryptedBlock(id, j)
}

// List returns the list of vault items matching the specified prefix.
func (v *Vault) List(prefix string) ([]string, error) {
	var result []string

	for b := range v.storage.ListBlocks(v.itemPrefix + prefix) {
		if b.Error != nil {
			return result, b.Error
		}

		itemID := strings.TrimPrefix(b.BlockID, v.itemPrefix)
		if !isReservedName(itemID) {
			result = append(result, itemID)
		}
	}
	return result, nil
}

// Close releases any resources held by Vault and closes repository connection.
func (v *Vault) Close() error {
	return nil
}

type vaultConfig struct {
	ConnectionInfo blob.ConnectionInfo `json:"connection"`
	Key            []byte              `json:"key,omitempty"`
}

// Token returns a persistent opaque string that encodes the configuration of vault storage
// and its credentials in a way that can be later used to open the vault.
func (v *Vault) Token() (string, error) {
	cip, ok := v.storage.(blob.ConnectionInfoProvider)
	if !ok {
		return "", errors.New("repository does not support persisting configuration")
	}

	ci := cip.ConnectionInfo()

	vc := vaultConfig{
		ConnectionInfo: ci,
		Key:            v.masterKey,
	}

	b, err := json.Marshal(&vc)
	if err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(b), nil
}

// Remove deletes the specified vault item.
func (v *Vault) Remove(itemID string) error {
	if err := checkReservedName(itemID); err != nil {
		return err
	}

	return v.storage.DeleteBlock(v.itemPrefix + itemID)
}

// Create initializes a Vault attached to the specified repository.
func Create(
	vaultStorage blob.Storage,
	vaultFormat *Format,
	vaultCreds Credentials,
	repoStorage blob.Storage,
	repoFormat *repo.Format,
) (*Vault, error) {
	v := Vault{
		storage: vaultStorage,
		format:  *vaultFormat,
	}

	if repoStorage == nil || sameStorage(repoStorage, vaultStorage) {
		repoStorage = vaultStorage
		v.itemPrefix = colocatedVaultItemPrefix
	}

	cip, ok := repoStorage.(blob.ConnectionInfoProvider)
	if !ok {
		return nil, errors.New("repository does not support persisting configuration")
	}

	v.format.Version = "1"
	v.format.UniqueID = make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, v.format.UniqueID); err != nil {
		return nil, err
	}
	v.masterKey = vaultCreds.getMasterKey(v.format.UniqueID)

	formatBytes, err := json.Marshal(&v.format)
	if err != nil {
		return nil, err
	}

	vaultStorage.PutBlock(
		v.itemPrefix+formatBlockID,
		blob.NewReader(bytes.NewBuffer(formatBytes)),
		blob.PutOptionsOverwrite,
	)

	// Write encrypted repository configuration block.
	rc := repositoryConfig{
		Format: repoFormat,
	}

	if repoStorage != vaultStorage {
		ci := cip.ConnectionInfo()
		rc.Connection = &ci
	}

	if err := v.putJSON(repositoryConfigBlockID, &rc); err != nil {
		return nil, err
	}

	v.repoConfig = rc
	return &v, nil
}

// Open opens a vault.
func Open(vaultStorage blob.Storage, vaultCreds Credentials) (*Vault, error) {
	v := Vault{
		storage: vaultStorage,
	}

	var prefix string

	f, err := vaultStorage.GetBlock(formatBlockID)
	if err == blob.ErrBlockNotFound {
		prefix = colocatedVaultItemPrefix
		f, err = vaultStorage.GetBlock(prefix + formatBlockID)
	}

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(f, &v.format)
	if err != nil {
		return nil, err
	}

	v.masterKey = vaultCreds.getMasterKey(v.format.UniqueID)
	v.itemPrefix = prefix

	var rc repositoryConfig
	if err := v.getJSON(repositoryConfigBlockID, &rc); err != nil {
		return nil, err
	}

	v.repoConfig = rc

	return &v, nil
}

// OpenWithToken opens a vault with storage configuration and credentials in the specified token.
func OpenWithToken(token string) (*Vault, error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("invalid vault base64 token: %v", err)
	}

	var vc vaultConfig
	err = json.Unmarshal(b, &vc)
	if err != nil {
		return nil, fmt.Errorf("invalid vault json token: %v", err)
	}

	st, err := blob.NewStorage(vc.ConnectionInfo)
	if err != nil {
		return nil, fmt.Errorf("cannot open vault storage: %v", err)
	}

	creds, err := MasterKey(vc.Key)
	if err != nil {
		return nil, fmt.Errorf("invalid vault token")
	}

	return Open(st, creds)
}

func isReservedName(itemID string) bool {
	switch itemID {
	case formatBlockID, repositoryConfigBlockID:
		return true

	default:
		return false
	}
}

func checkReservedName(itemID string) error {
	if isReservedName(itemID) {
		return fmt.Errorf("invalid vault item name: '%v'", itemID)
	}

	return nil
}

func sameStorage(s1, s2 blob.Storage) bool {
	if s1 == s2 {
		return true
	}

	cip1, ok := s1.(blob.ConnectionInfoProvider)
	if !ok {
		return false
	}

	cip2, ok := s2.(blob.ConnectionInfoProvider)
	if !ok {
		return false
	}

	c1 := cip1.ConnectionInfo()
	c2 := cip2.ConnectionInfo()

	b1, e1 := json.Marshal(c1)
	b2, e2 := json.Marshal(c2)

	same := e1 == nil && e2 == nil && bytes.Equal(b1, b2)
	return same
}
