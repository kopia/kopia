package metadata

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/storage"
)

const (
	parallelFetches = 5
)

var (
	purposeAESKey   = []byte("AES")
	purposeAuthData = []byte("CHECKSUM")
)

// ErrNotFound is an error returned when a metadata item cannot be found.
var ErrNotFound = errors.New("metadata not found")

// SupportedEncryptionAlgorithms is a list of supported metadata encryption algorithms.
var SupportedEncryptionAlgorithms = []string{
	"AES256_GCM",
	"NONE",
}

// DefaultEncryptionAlgorithm is a metadata encryption algorithm used for new repositories.
const DefaultEncryptionAlgorithm = "AES256_GCM"

// Format describes the format of metadata items in repository.
// Contents of this structure are serialized in plain text in the storage.
type Format struct {
	Version             string `json:"version"`
	EncryptionAlgorithm string `json:"encryption"`
}

// Manager manages JSON metadata, such as snapshot manifests, policies, object format etc.
// in a repository.
type Manager struct {
	Format Format

	storage storage.Storage
	cache   *metadataCache

	aead     cipher.AEAD // authenticated encryption to use
	authData []byte      // additional data to authenticate
}

// Put saves the specified metadata content under a provided name.
func (m *Manager) Put(itemID string, content []byte) error {
	if err := checkReservedName(itemID); err != nil {
		return err
	}

	return m.writeEncryptedBlock(itemID, content)
}

// RefreshCache refreshes the cache of metadata items.
func (m *Manager) RefreshCache() error {
	return m.cache.refresh()
}

func (m *Manager) writeEncryptedBlock(itemID string, content []byte) error {
	if m.aead != nil {
		nonceLength := m.aead.NonceSize()
		noncePlusContentLength := nonceLength + len(content)
		cipherText := make([]byte, noncePlusContentLength+m.aead.Overhead())

		// Store nonce at the beginning of ciphertext.
		nonce := cipherText[0:nonceLength]
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return err
		}

		b := m.aead.Seal(cipherText[nonceLength:nonceLength], nonce, content, m.authData)

		content = nonce[0 : nonceLength+len(b)]
	}

	return m.cache.PutBlock(itemID, content)
}

func (m *Manager) readEncryptedBlock(itemID string) ([]byte, error) {
	content, err := m.cache.GetBlock(itemID)
	if err != nil {
		if err == storage.ErrBlockNotFound {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("unexpected error reading %v: %v", itemID, err)
	}

	return m.decryptBlock(content)
}

func (m *Manager) decryptBlock(content []byte) ([]byte, error) {
	if m.aead != nil {
		nonce := content[0:m.aead.NonceSize()]
		payload := content[m.aead.NonceSize():]
		return m.aead.Open(payload[:0], nonce, payload, m.authData)
	}

	return content, nil
}

// GetMetadata returns the contents of a specified metadata item.
func (m *Manager) GetMetadata(itemID string) ([]byte, error) {
	if err := checkReservedName(itemID); err != nil {
		return nil, err
	}

	return m.readEncryptedBlock(itemID)
}

// MultiGet gets the contents of a specified multiple metadata items efficiently.
// The results are returned as a map, with items that are not found not present in the map.
func (m *Manager) MultiGet(itemIDs []string) (map[string][]byte, error) {
	type singleReadResult struct {
		id       string
		contents []byte
		err      error
	}

	ch := make(chan singleReadResult)
	inputs := make(chan string)
	for i := 0; i < parallelFetches; i++ {
		go func() {
			for itemID := range inputs {
				v, err := m.GetMetadata(itemID)
				ch <- singleReadResult{itemID, v, err}
			}
		}()
	}

	go func() {
		// feed item IDs to workers.
		for _, i := range itemIDs {
			inputs <- i
		}
		close(inputs)
	}()

	// fetch exactly N results
	var resultErr error
	resultMap := make(map[string][]byte)
	for i := 0; i < len(itemIDs); i++ {
		r := <-ch
		if r.err != nil {
			resultErr = r.err
		} else {
			resultMap[r.id] = r.contents
		}
	}

	if resultErr != nil {
		return nil, resultErr
	}

	return resultMap, nil
}

// GetJSON reads and parses given item as JSON.
func (m *Manager) GetJSON(itemID string, content interface{}) error {
	j, err := m.readEncryptedBlock(itemID)
	if err != nil {
		return err
	}

	return json.Unmarshal(j, content)
}

// PutJSON stores the contents of an item stored with a given ID.
func (m *Manager) PutJSON(id string, content interface{}) error {
	j, err := json.Marshal(content)
	if err != nil {
		return err
	}

	return m.writeEncryptedBlock(id, j)
}

// List returns the list of metadata items matching the specified prefix.
func (m *Manager) List(prefix string) ([]string, error) {
	return m.cache.ListBlocks(prefix)
}

// ListContents retrieves metadata contents for all items starting with a given prefix.
func (m *Manager) ListContents(prefix string) (map[string][]byte, error) {
	itemIDs, err := m.List(prefix)
	if err != nil {
		return nil, err
	}

	return m.MultiGet(itemIDs)
}

// Remove removes the specified metadata item.
func (m *Manager) Remove(itemID string) error {
	if err := checkReservedName(itemID); err != nil {
		return err
	}

	return m.cache.DeleteBlock(itemID)
}

// RemoveMany efficiently removes multiple metadata items in parallel.
func (m *Manager) RemoveMany(itemIDs []string) error {
	parallelism := 30
	ch := make(chan string)
	var wg sync.WaitGroup
	errch := make(chan error, len(itemIDs))

	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for id := range ch {
				if err := m.Remove(id); err != nil {
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

// NewManager opens a MetadataManager for given storage and credentials.
func NewManager(st storage.Storage, f Format, km *auth.KeyManager) (*Manager, error) {
	cache, err := newMetadataCache(st)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		Format:  f,
		storage: st,
		cache:   cache,
	}

	if err := m.initCrypto(f, km); err != nil {
		return nil, fmt.Errorf("unable to initialize crypto: %v", err)
	}

	return m, nil
}

func (m *Manager) initCrypto(f Format, km *auth.KeyManager) error {
	switch f.EncryptionAlgorithm {
	case "NONE": // do nothing
		return nil
	case "AES256_GCM":
		aesKey := km.DeriveKey(purposeAESKey, 32)
		m.authData = km.DeriveKey(purposeAuthData, 32)

		blk, err := aes.NewCipher(aesKey)
		if err != nil {
			return fmt.Errorf("cannot create cipher: %v", err)
		}
		m.aead, err = cipher.NewGCM(blk)
		if err != nil {
			return fmt.Errorf("cannot create cipher: %v", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}
}

func isReservedName(itemID string) bool {
	switch itemID {
	case "format", "repo":
		return true

	default:
		return false
	}
}

func checkReservedName(itemID string) error {
	if isReservedName(itemID) {
		return fmt.Errorf("invalid metadata item name: '%v'", itemID)
	}

	return nil
}
