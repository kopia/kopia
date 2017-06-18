package vault

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/repo"

	"golang.org/x/crypto/hkdf"
)

const (
	formatBlockID           = "format"
	repositoryConfigBlockID = "repo"
)

const (
	// ColocatedBlockPrefix is a prefix used for colocated vault blocks in a repository storage.
	ColocatedBlockPrefix = "VLT"
)

var (
	purposeAESKey   = []byte("AES")
	purposeAuthData = []byte("CHECKSUM")
)

// ErrItemNotFound is an error returned when a vault item cannot be found.
var ErrItemNotFound = errors.New("item not found")

// SupportedEncryptionAlgorithms lists supported key derivation algorithms.
var SupportedEncryptionAlgorithms = []string{
	"AES256_GCM",
	"NONE",
}

// Vault is a secure storage for secrets such as repository object identifiers.
type Vault struct {
	storage    blob.Storage
	format     Format
	RepoConfig RepositoryConfig

	masterKey  []byte
	itemPrefix string

	aead     cipher.AEAD // authenticated encryption to use
	authData []byte      // additional data to authenticate
}

// Put saves the specified content in a vault under a specified name.
func (v *Vault) Put(itemID string, content []byte) error {
	if err := checkReservedName(itemID); err != nil {
		return err
	}

	return v.writeEncryptedBlock(itemID, content)
}

func (v *Vault) writeEncryptedBlock(itemID string, content []byte) error {
	if v.aead != nil {
		nonceLength := v.aead.NonceSize()
		noncePlusContentLength := nonceLength + len(content)
		cipherText := make([]byte, noncePlusContentLength+v.aead.Overhead())

		// Store nonce at the beginning of ciphertext.
		nonce := cipherText[0:nonceLength]
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return err
		}

		b := v.aead.Seal(cipherText[nonceLength:nonceLength], nonce, content, v.authData)

		content = nonce[0 : nonceLength+len(b)]
	}

	return v.storage.PutBlock(v.itemPrefix+itemID, content, blob.PutOptionsOverwrite)
}

func (v *Vault) readEncryptedBlock(itemID string) ([]byte, error) {
	content, err := v.storage.GetBlock(v.itemPrefix + itemID)
	if err != nil {
		if err == blob.ErrBlockNotFound {
			return nil, ErrItemNotFound
		}
		return nil, fmt.Errorf("unexpected error reading %v: %v", itemID, err)
	}

	return v.decryptBlock(content)
}

func (v *Vault) decryptBlock(content []byte) ([]byte, error) {
	if v.aead != nil {
		nonce := content[0:v.aead.NonceSize()]
		payload := content[v.aead.NonceSize():]
		return v.aead.Open(payload[:0], nonce, payload, v.authData)
	}

	return content, nil
}

// DeriveKey computes a key for a specific purpose and length using HKDF based on the master key.
func (v *Vault) DeriveKey(purpose []byte, length int) []byte {
	key := make([]byte, length)
	k := hkdf.New(sha256.New, v.masterKey, v.format.UniqueID, purpose)
	io.ReadFull(k, key)
	return key
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
// The 'limit' parameter specifies the maximum number of items to retrieve (-1 == unlimited).
func (v *Vault) List(prefix string, limit int) ([]string, error) {
	var result []string

	for b := range v.storage.ListBlocks(v.itemPrefix+prefix, limit) {
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
	return v.storage.Close()
}

// Config represents JSON-compatible configuration of the vault connection, including vault key.
type Config struct {
	ConnectionInfo blob.ConnectionInfo `json:"connection"`
	Key            []byte              `json:"key,omitempty"`
}

// Config returns a configuration of vault storage its credentials that's suitable
// for storing in configuration file.
func (v *Vault) Config() (*Config, error) {
	cip, ok := v.storage.(blob.ConnectionInfoProvider)
	if !ok {
		return nil, errors.New("repository does not support persisting configuration")
	}

	ci := cip.ConnectionInfo()

	return &Config{
		ConnectionInfo: ci,
		Key:            v.masterKey,
	}, nil
}

// Remove deletes the specified vault item.
func (v *Vault) Remove(itemID string) error {
	if err := checkReservedName(itemID); err != nil {
		return err
	}

	return v.storage.DeleteBlock(v.itemPrefix + itemID)
}

// RemoveMany efficiently removes multiple vault items in parallel.
func (v *Vault) RemoveMany(itemIDs []string) error {
	parallelism := 30
	ch := make(chan string)
	var wg sync.WaitGroup
	errch := make(chan error, len(itemIDs))

	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for id := range ch {
				if err := v.Remove(id); err != nil {
					errch <- err
				}
			}
		}()
	}

	for _, id := range itemIDs {
		ch <- id
	}
	close(ch)
	wg.Wait()
	close(errch)

	return <-errch
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

	if repoStorage == nil {
		repoStorage = vaultStorage
		v.itemPrefix = ColocatedBlockPrefix
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
	if v.format.KeyAlgorithm == "" {
		v.format.KeyAlgorithm = defaultKeyAlgorithm

	}
	var err error
	v.masterKey, err = vaultCreds.getMasterKey(&v.format)
	if err != nil {
		return nil, err
	}

	formatBytes, err := json.Marshal(&v.format)
	if err != nil {
		return nil, err
	}

	if err := vaultStorage.PutBlock(v.itemPrefix+formatBlockID, formatBytes, blob.PutOptionsOverwrite); err != nil {
		return nil, err
	}

	if err := v.initCrypto(); err != nil {
		return nil, fmt.Errorf("unable to initialize crypto: %v", err)
	}

	// Write encrypted repository configuration block.
	rc := RepositoryConfig{
		Format: repoFormat,
	}

	if repoStorage != vaultStorage {
		ci := cip.ConnectionInfo()
		rc.Connection = &ci
	}

	if err := v.putJSON(repositoryConfigBlockID, &rc); err != nil {
		return nil, err
	}

	v.RepoConfig = rc
	return &v, nil
}

// CreateColocated initializes a Vault attached to a Repository sharing the same storage.
func CreateColocated(
	sharedStorage blob.Storage,
	vaultFormat *Format,
	vaultCreds Credentials,
	repoFormat *repo.Format,
) (*Vault, error) {
	return Create(sharedStorage, vaultFormat, vaultCreds, nil, repoFormat)
}

// RepositoryConfig stores the configuration of the repository associated with the vault.
type RepositoryConfig struct {
	Connection *blob.ConnectionInfo `json:"connection"`
	Format     *repo.Format         `json:"format"`
}

// Open opens a vault.
func Open(vaultStorage blob.Storage, vaultCreds Credentials) (*Vault, error) {
	v := Vault{
		storage: vaultStorage,
	}

	var prefix string
	var wg sync.WaitGroup

	var blocks [4][]byte

	f := func(index int, name string) {
		blocks[index], _ = vaultStorage.GetBlock(name)
		wg.Done()
	}

	wg.Add(4)
	go f(0, formatBlockID)
	go f(1, repositoryConfigBlockID)
	go f(2, ColocatedBlockPrefix+formatBlockID)
	go f(3, ColocatedBlockPrefix+repositoryConfigBlockID)
	wg.Wait()

	if blocks[0] == nil && blocks[2] == nil {
		return nil, fmt.Errorf("vault format block not found")
	}

	var offset = 0
	if blocks[0] == nil {
		prefix = ColocatedBlockPrefix
		offset = 2
	}

	err := json.Unmarshal(blocks[offset], &v.format)
	if err != nil {
		return nil, err
	}

	v.masterKey, err = vaultCreds.getMasterKey(&v.format)
	if err != nil {
		return nil, err
	}
	v.itemPrefix = prefix

	if err := v.initCrypto(); err != nil {
		return nil, fmt.Errorf("unable to initialize crypto: %v", err)
	}

	cfgData, err := v.decryptBlock(blocks[offset+1])
	if err != nil {
		return nil, err
	}

	var rc RepositoryConfig

	if err := json.Unmarshal(cfgData, &rc); err != nil {
		return nil, err
	}

	v.RepoConfig = rc

	return &v, nil
}

func (v *Vault) initCrypto() error {
	switch v.format.EncryptionAlgorithm {
	case "NONE": // do nothing
		return nil
	case "AES256_GCM":
		aesKey := v.DeriveKey(purposeAESKey, 32)
		v.authData = v.DeriveKey(purposeAuthData, 32)

		blk, err := aes.NewCipher(aesKey)
		if err != nil {
			return fmt.Errorf("cannot create cipher: %v", err)
		}
		v.aead, err = cipher.NewGCM(blk)
		if err != nil {
			return fmt.Errorf("cannot create cipher: %v", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown encryption algorithm: '%v'", v.format.EncryptionAlgorithm)
	}
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
