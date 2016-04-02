package cas

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"strings"
	"sync/atomic"

	"github.com/kopia/kopia/storage"
)

// Since we never share keys, using constant IV is fine.
// Instead of using all-zero, we use this one.
var constantIV = []byte("kopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopia")

// ObjectManager manages objects stored in a repository and allows reading and writing them.
type ObjectManager interface {
	// NewWriter opens an ObjectWriter for writing new content to the repository.
	NewWriter(options ...WriterOption) ObjectWriter

	// Open creates an io.ReadSeeker for reading object with a specified ID.
	Open(objectID ObjectID) (io.ReadSeeker, error)

	Flush() error
	Repository() storage.Repository
	Close()

	Stats() ObjectManagerStats
}

// ObjectManagerStats exposes statistics about ObjectManager operation
type ObjectManagerStats struct {
	HashedBytes   int64
	HashedBlocks  int32
	UploadedBytes int64
}

type keygenFunc func([]byte) (key []byte, locator []byte)

type objectManager struct {
	repository    storage.Repository
	verbose       bool
	bufferManager *bufferManager
	stats         ObjectManagerStats

	maxInlineBlobSize int
	maxBlobSize       int

	hashFunc     func() hash.Hash
	createCipher func([]byte) (cipher.Block, error)
	keygen       keygenFunc
}

func (mgr *objectManager) Close() {
	mgr.Flush()
	mgr.bufferManager.close()
}

func (mgr *objectManager) Flush() error {
	return mgr.repository.Flush()
}

func (mgr *objectManager) Stats() ObjectManagerStats {
	return mgr.stats
}

func (mgr *objectManager) Repository() storage.Repository {
	return mgr.repository
}

func (mgr *objectManager) NewWriter(options ...WriterOption) ObjectWriter {
	result := newObjectWriter(
		objectWriterConfig{
			mgr:        mgr,
			putOptions: storage.PutOptions{},
		},
		ObjectIDTypeStored)

	for _, option := range options {
		option(result)
	}

	return result
}

func (mgr *objectManager) Open(objectID ObjectID) (io.ReadSeeker, error) {
	r, err := mgr.newRawReader(objectID)
	if err != nil {
		return nil, err
	}

	if objectID.Type() == ObjectIDTypeList {
		seekTable := make([]seekTableEntry, 0, 100)

		seekTable, err = mgr.flattenListChunk(seekTable, objectID, r)
		if err != nil {
			return nil, err
		}

		totalLength := seekTable[len(seekTable)-1].endOffset()

		return &objectReader{
			repository:  mgr.repository,
			seekTable:   seekTable,
			totalLength: totalLength,
		}, nil
	}
	return r, err
}

// ObjectManagerOption controls the behavior of ObjectManager.
type ObjectManagerOption func(o *objectManager) error

// WriteBack is an ObjectManagerOption that enables asynchronous writes to the repository using the pool
// of goroutines.
func WriteBack(workerCount int) ObjectManagerOption {
	return func(o *objectManager) error {
		o.repository = storage.NewWriteBackWrapper(o.repository, workerCount)
		return nil
	}
}

// WriteLimit is an ObjectManagerOption that sets the limit on the number of bytes that can be written
// to the repository in this ObjectManager session. Once the limit is reached, the repository will
// return ErrWriteLimitExceeded.
func WriteLimit(maxBytes int64) ObjectManagerOption {
	return func(o *objectManager) error {
		o.repository = storage.NewWriteLimitWrapper(o.repository, maxBytes)
		return nil
	}
}

// EnableLogging is an ObjectManagerOption that causes all repository access to be logged.
func EnableLogging() ObjectManagerOption {
	return func(o *objectManager) error {
		o.repository = storage.NewLoggingWrapper(o.repository)
		return nil
	}
}

// NewObjectManager creates new ObjectManager with the specified repository, options, and key provider.
func NewObjectManager(
	r storage.Repository,
	f Format,
	options ...ObjectManagerOption,
) (ObjectManager, error) {
	if f.Version != "1" {
		return nil, fmt.Errorf("unsupported repository version: %v", f.Version)
	}
	mgr := &objectManager{
		repository:        r,
		maxInlineBlobSize: f.MaxInlineBlobSize,
		maxBlobSize:       f.MaxBlobSize,
	}

	if mgr.maxBlobSize == 0 {
		mgr.maxBlobSize = 16 * 1024 * 1024
	}

	var hashFunc func() hash.Hash

	hashAlgo := f.Hash
	hf := strings.TrimPrefix(hashAlgo, "hmac-")

	switch hf {
	case "md5":
		hashFunc = md5.New
	case "sha1":
		hashFunc = sha1.New
	case "sha256":
		hashFunc = sha256.New
	case "sha512":
		hashFunc = sha512.New
	default:
		return nil, fmt.Errorf("unknown hash function: %v", hf)
	}

	if strings.HasPrefix(hashAlgo, "hmac-") {
		rawHashFunc := hashFunc
		hashFunc = func() hash.Hash {
			return hmac.New(rawHashFunc, f.Secret)
		}
	}

	mgr.hashFunc = hashFunc

	switch f.Encryption {
	case "aes-128":
		mgr.createCipher = aes.NewCipher
		mgr.keygen = splitHash(16)
	case "aes-192":
		mgr.createCipher = aes.NewCipher
		mgr.keygen = splitHash(24)
	case "aes-256":
		mgr.createCipher = aes.NewCipher
		mgr.keygen = splitHash(32)
	}

	for _, o := range options {
		if err := o(mgr); err != nil {
			mgr.Close()
			return nil, err
		}
	}

	mgr.bufferManager = newBufferManager(mgr.maxBlobSize)

	return mgr, nil
}

func splitHash(keySize int) keygenFunc {
	return func(b []byte) ([]byte, []byte) {
		p := len(b) - keySize
		return b[p:], b[0:p]
	}
}

func (mgr *objectManager) hashBufferForWriting(buffer *bytes.Buffer, prefix string) (ObjectID, io.ReadCloser) {
	var data []byte
	if buffer != nil {
		data = buffer.Bytes()
	}

	h := mgr.hashFunc()
	h.Write(data)
	contentHash := h.Sum(nil)

	var objectID ObjectID
	var cryptoKey []byte

	if mgr.createCipher != nil {
		cryptoKey, contentHash = mgr.keygen(contentHash)
		objectID = ObjectID(prefix + hex.EncodeToString(contentHash) + ":" + hex.EncodeToString(cryptoKey))
	} else {
		objectID = ObjectID(prefix + hex.EncodeToString(contentHash))
	}

	atomic.AddInt64(&mgr.stats.HashedBytes, int64(len(data)))

	if buffer == nil {
		return objectID, ioutil.NopCloser(bytes.NewBuffer(nil))
	}

	readCloser := mgr.bufferManager.returnBufferOnClose(buffer)

	if cryptoKey != nil {
		c, err := mgr.createCipher(cryptoKey)
		if err != nil {
			panic("can't create cipher")
		}

		// Since we're not sharing the key, all-zero IV is ok.
		// We don't need to worry about separate MAC either, since hashing content produces object ID.
		ctr := cipher.NewCTR(c, constantIV[0:c.BlockSize()])

		readCloser = newEncryptingReader(readCloser, nil, ctr, nil)
	}

	return objectID, readCloser
}
