package repo

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/internal/config"

	"golang.org/x/crypto/hkdf"
)

const (
	formatBlockID           = "format"
	repositoryConfigBlockID = "repo"
)

const (
	parallelFetches = 5
)

var (
	purposeAESKey   = []byte("AES")
	purposeAuthData = []byte("CHECKSUM")
)

// ErrMetadataNotFound is an error returned when a metadata item cannot be found.
var ErrMetadataNotFound = errors.New("metadata not found")

// SupportedMetadataEncryptionAlgorithms is a list of supported metadata encryption algorithms including:
//
//   AES256_GCM    - AES-256 in GCM mode
//   NONE          - no encryption
var SupportedMetadataEncryptionAlgorithms []string

// DefaultMetadataEncryptionAlgorithm is a metadata encryption algorithm used for new repositories.
const DefaultMetadataEncryptionAlgorithm = "AES256_GCM"

func init() {
	SupportedMetadataEncryptionAlgorithms = []string{
		"AES256_GCM",
		"NONE",
	}
}

// MetadataManager manages JSON metadata, such as snapshot manifests, policies, object format etc.
// in a repository.
type MetadataManager struct {
	storage    blob.Storage
	cache      *metadataCache
	format     config.MetadataFormat
	repoConfig config.EncryptedRepositoryConfig

	masterKey []byte

	aead     cipher.AEAD // authenticated encryption to use
	authData []byte      // additional data to authenticate
}

// PutMetadata saves the specified metadata content under a provided name.
func (mm *MetadataManager) PutMetadata(itemID string, content []byte) error {
	if err := checkReservedName(itemID); err != nil {
		return err
	}

	return mm.writeEncryptedBlock(itemID, content)
}

func (mm *MetadataManager) writeEncryptedBlock(itemID string, content []byte) error {
	if mm.aead != nil {
		nonceLength := mm.aead.NonceSize()
		noncePlusContentLength := nonceLength + len(content)
		cipherText := make([]byte, noncePlusContentLength+mm.aead.Overhead())

		// Store nonce at the beginning of ciphertext.
		nonce := cipherText[0:nonceLength]
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return err
		}

		b := mm.aead.Seal(cipherText[nonceLength:nonceLength], nonce, content, mm.authData)

		content = nonce[0 : nonceLength+len(b)]
	}

	return mm.cache.PutBlock(itemID, content)
}

func (mm *MetadataManager) readEncryptedBlock(itemID string) ([]byte, error) {
	content, err := mm.cache.GetBlock(itemID)
	if err != nil {
		if err == blob.ErrBlockNotFound {
			return nil, ErrMetadataNotFound
		}
		return nil, fmt.Errorf("unexpected error reading %v: %v", itemID, err)
	}

	return mm.decryptBlock(content)
}

func (mm *MetadataManager) decryptBlock(content []byte) ([]byte, error) {
	if mm.aead != nil {
		nonce := content[0:mm.aead.NonceSize()]
		payload := content[mm.aead.NonceSize():]
		return mm.aead.Open(payload[:0], nonce, payload, mm.authData)
	}

	return content, nil
}

// DeriveKey computes a key for a specific purpose and length using HKDF based on the master key.
func (mm *MetadataManager) DeriveKey(purpose []byte, length int) []byte {
	key := make([]byte, length)
	k := hkdf.New(sha256.New, mm.masterKey, mm.format.UniqueID, purpose)
	io.ReadFull(k, key)
	return key
}

// GetMetadata returns the contents of a specified metadata item.
func (mm *MetadataManager) GetMetadata(itemID string) ([]byte, error) {
	if err := checkReservedName(itemID); err != nil {
		return nil, err
	}

	return mm.readEncryptedBlock(itemID)
}

// MultiGetMetadata gets the contents of a specified multiple metadata items efficiently.
// The results are returned as a map, with items that are not found not present in the map.
func (mm *MetadataManager) MultiGetMetadata(itemIDs []string) (map[string][]byte, error) {
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
				v, err := mm.GetMetadata(itemID)
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

func (mm *MetadataManager) getJSON(itemID string, content interface{}) error {
	j, err := mm.readEncryptedBlock(itemID)
	if err != nil {
		return err
	}

	return json.Unmarshal(j, content)
}

// putJSON stores the contents of an item stored with a given ID.
func (mm *MetadataManager) putJSON(id string, content interface{}) error {
	j, err := json.Marshal(content)
	if err != nil {
		return err
	}

	return mm.writeEncryptedBlock(id, j)
}

// ListMetadata returns the list of metadata items matching the specified prefix.
func (mm *MetadataManager) ListMetadata(prefix string) ([]string, error) {
	return mm.cache.ListBlocks(prefix)
}

// ListMetadataContents retrieves metadata contents for all items starting with a given prefix.
func (mm *MetadataManager) ListMetadataContents(prefix string) (map[string][]byte, error) {
	itemIDs, err := mm.ListMetadata(prefix)
	if err != nil {
		return nil, err
	}

	return mm.MultiGetMetadata(itemIDs)
}

// Config returns a configuration of storage its credentials that's suitable
// for storing in configuration file.
func (mm *MetadataManager) connectionConfiguration() (*config.RepositoryConnectionInfo, error) {
	cip, ok := mm.storage.(blob.ConnectionInfoProvider)
	if !ok {
		return nil, errors.New("repository does not support persisting configuration")
	}

	ci := cip.ConnectionInfo()

	return &config.RepositoryConnectionInfo{
		ConnectionInfo: ci,
		Key:            mm.masterKey,
	}, nil
}

// RemoveMetadata removes the specified metadata item.
func (mm *MetadataManager) RemoveMetadata(itemID string) error {
	if err := checkReservedName(itemID); err != nil {
		return err
	}

	return mm.cache.DeleteBlock(itemID)
}

// RemoveMany efficiently removes multiple metadata items in parallel.
func (mm *MetadataManager) RemoveMany(itemIDs []string) error {
	parallelism := 30
	ch := make(chan string)
	var wg sync.WaitGroup
	errch := make(chan error, len(itemIDs))

	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for id := range ch {
				if err := mm.RemoveMetadata(id); err != nil {
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

// newMetadataManager opens a MetadataManager for given storage and credentials.
func newMetadataManager(st blob.Storage, creds auth.Credentials) (*MetadataManager, error) {
	cache, err := newMetadataCache(st)
	if err != nil {
		return nil, err
	}

	mm := MetadataManager{
		storage: st,
		cache:   cache,
	}

	var wg sync.WaitGroup

	var blocks [4][]byte

	f := func(index int, name string) {
		blocks[index], _ = mm.cache.GetBlock(name)
		wg.Done()
	}

	wg.Add(2)
	go f(0, formatBlockID)
	go f(1, repositoryConfigBlockID)
	wg.Wait()

	if blocks[0] == nil {
		return nil, fmt.Errorf("format block not found")
	}

	var offset = 0
	err = json.Unmarshal(blocks[offset], &mm.format)
	if err != nil {
		return nil, err
	}

	mm.masterKey, err = creds.GetMasterKey(mm.format.SecurityOptions)
	if err != nil {
		return nil, err
	}

	if err := mm.initCrypto(); err != nil {
		return nil, fmt.Errorf("unable to initialize crypto: %v", err)
	}

	cfgData, err := mm.decryptBlock(blocks[offset+1])
	if err != nil {
		return nil, err
	}

	var rc config.EncryptedRepositoryConfig

	if err := json.Unmarshal(cfgData, &rc); err != nil {
		return nil, err
	}

	mm.repoConfig = rc

	return &mm, nil
}

func (mm *MetadataManager) initCrypto() error {
	switch mm.format.EncryptionAlgorithm {
	case "NONE": // do nothing
		return nil
	case "AES256_GCM":
		aesKey := mm.DeriveKey(purposeAESKey, 32)
		mm.authData = mm.DeriveKey(purposeAuthData, 32)

		blk, err := aes.NewCipher(aesKey)
		if err != nil {
			return fmt.Errorf("cannot create cipher: %v", err)
		}
		mm.aead, err = cipher.NewGCM(blk)
		if err != nil {
			return fmt.Errorf("cannot create cipher: %v", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown encryption algorithm: '%v'", mm.format.EncryptionAlgorithm)
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
		return fmt.Errorf("invalid metadata item name: '%v'", itemID)
	}

	return nil
}
