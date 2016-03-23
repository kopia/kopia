package cas

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"io"
	"strings"

	"github.com/kopia/kopia/content"
	"github.com/kopia/kopia/storage"
)

// ObjectManager manages objects stored in a repository and allows reading and writing them.
type ObjectManager interface {
	// NewWriter opens an ObjectWriter for writing new content to the repository.
	NewWriter(options ...WriterOption) ObjectWriter

	// Open creates an io.ReadSeeker for reading object with a specified ID.
	Open(objectID content.ObjectID) (io.ReadSeeker, error)

	Flush() error
	Repository() storage.Repository
	Close()
}

type objectManager struct {
	repository    storage.Repository
	verbose       bool
	formatter     objectFormatter
	bufferManager *bufferManager

	maxInlineBlobSize int
	maxBlobSize       int
}

func (mgr *objectManager) Close() {
	mgr.Flush()
	mgr.bufferManager.close()
}

func (mgr *objectManager) Flush() error {
	return mgr.repository.Flush()
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
		content.ObjectIDTypeStored)

	for _, option := range options {
		option(result)
	}

	return result
}

func (mgr *objectManager) Open(objectID content.ObjectID) (io.ReadSeeker, error) {
	r, err := mgr.newRawReader(objectID)
	if err != nil {
		return nil, err
	}

	if objectID.Type() == content.ObjectIDTypeList {
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

	mgr.formatter = newNonEncryptingFormatter(hashFunc)

	for _, o := range options {
		if err := o(mgr); err != nil {
			mgr.Close()
			return nil, err
		}
	}

	mgr.bufferManager = newBufferManager(mgr.maxBlobSize)

	return mgr, nil
}
