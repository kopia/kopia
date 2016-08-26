package repo

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/kopia/kopia/internal/jsonstream"
	"github.com/kopia/kopia/storage"
	"github.com/kopia/kopia/storage/logging"
)

const (
	minLocatorSizeBytes = 16
)

// ObjectReader allows reading, seeking, getting the length of and closing of a repository object.
type ObjectReader interface {
	io.Reader
	io.Seeker
	io.Closer
	Length() int64
}

// Repository objects addressed by their content allows reading and writing them.
type Repository interface {
	// NewWriter opens an ObjectWriter for writing new content to the storage.
	NewWriter(options ...WriterOption) ObjectWriter

	// Open creates an ObjectReader for reading object with a specified ID.
	Open(objectID ObjectID) (ObjectReader, error)

	// Flush ensures that all pending writes have completed.
	Flush() error

	// Storage returns the underlying Storage.
	Storage() storage.Storage

	// Close
	Close() error

	Stats() RepositoryStats
}

// RepositoryStats exposes statistics about Repository operation
type RepositoryStats struct {
	// Keep int64 fields first to ensure they get aligned to at least 64-bit boundaries
	// which is required for atomic access on ARM and x86-32.
	HashedBytes           int64
	BytesReadFromStorage  int64
	BytesWrittenToStorage int64
	EncryptedBytes        int64
	DecryptedBytes        int64

	HashedBlocks           int32
	BlocksReadFromStorage  int32
	BlocksWrittenToStorage int32
	InvalidBlocks          int32
	ValidBlocks            int32
}

type empty struct{}
type semaphore chan empty

func (s semaphore) Lock() {
	s <- empty{}
}

func (s semaphore) Unlock() {
	<-s
}

type repository struct {
	storage       storage.Storage
	verbose       bool
	bufferManager *bufferManager
	stats         *RepositoryStats

	format    Format
	formatter ObjectFormatter

	writeBackWorkers int

	writeBackSemaphore semaphore
	writeBackErrors    asyncErrors

	waitGroup sync.WaitGroup
}

type asyncErrors struct {
	sync.RWMutex
	errors []error
}

func (e *asyncErrors) add(err error) {
	e.Lock()
	e.errors = append(e.errors, err)
	e.Unlock()
}

func (e *asyncErrors) check() error {
	e.RLock()
	defer e.RUnlock()

	switch len(e.errors) {
	case 0:
		return nil
	case 1:
		return e.errors[0]
	default:
		msg := make([]string, len(e.errors))
		for i, err := range e.errors {
			msg[i] = err.Error()
		}

		return fmt.Errorf("%v errors: %v", len(e.errors), strings.Join(msg, ";"))
	}
}

func (r *repository) Close() error {
	r.Flush()
	if err := r.storage.Close(); err != nil {
		return err
	}
	r.bufferManager.close()

	return nil
}

func (r *repository) Flush() error {
	if r.writeBackWorkers > 0 {
		r.waitGroup.Wait()
	}
	return nil
}

func (r *repository) Stats() RepositoryStats {
	return *r.stats
}

func (r *repository) Storage() storage.Storage {
	return r.storage
}

func (r *repository) NewWriter(options ...WriterOption) ObjectWriter {
	result := &objectWriter{
		repo:         r,
		blockTracker: &blockTracker{},
	}

	for _, option := range options {
		option(result)
	}

	return result
}

func (r *repository) Open(objectID ObjectID) (ObjectReader, error) {
	// log.Printf("Repository::Open %v", objectID.String())
	// defer log.Printf("finished Repository::Open() %v", objectID.String())

	// Flush any pending writes.
	r.Flush()

	if objectID.Section != nil {
		baseReader, err := r.Open(objectID.Section.Base)
		if err != nil {
			return nil, fmt.Errorf("cannot create base reader: %+v %v", objectID.Section.Base, err)
		}

		return newObjectSectionReader(objectID.Section.Start, objectID.Section.Length, baseReader)
	}

	if objectID.Indirect > 0 {
		rd, err := r.Open(removeIndirection(objectID))
		if err != nil {
			return nil, err
		}
		defer r.Close()

		seekTable, err := r.flattenListChunk(rd)
		if err != nil {
			return nil, err
		}

		totalLength := seekTable[len(seekTable)-1].endOffset()

		return &objectReader{
			repo:        r,
			seekTable:   seekTable,
			totalLength: totalLength,
		}, nil
	}

	return r.newRawReader(objectID)
}

// RepositoryOption controls the behavior of Repository.
type RepositoryOption func(o *repository) error

// WriteBack is an RepositoryOption that enables asynchronous writes to the storage using the pool
// of goroutines.
func WriteBack(writeBackWorkers int) RepositoryOption {
	return func(o *repository) error {
		o.writeBackWorkers = writeBackWorkers
		return nil
	}
}

// EnableLogging is an RepositoryOption that causes all storage access to be logged.
func EnableLogging(options ...logging.Option) RepositoryOption {
	return func(o *repository) error {
		o.storage = logging.NewWrapper(o.storage, options...)
		return nil
	}
}

// New creates a Repository with the specified storage, format and options.
func New(s storage.Storage, f *Format, options ...RepositoryOption) (Repository, error) {
	if f.MaxBlockSize < 100 {
		return nil, fmt.Errorf("MaxBlockSize is not set")
	}

	sf := SupportedFormats[f.ObjectFormat]
	if sf == nil {
		return nil, fmt.Errorf("unknown object format: %v", f.ObjectFormat)
	}

	r := &repository{
		storage: s,
		format:  *f,
		stats:   &RepositoryStats{},
	}

	r.formatter = sf

	for _, o := range options {
		if err := o(r); err != nil {
			r.Close()
			return nil, err
		}
	}

	r.bufferManager = newBufferManager(int(r.format.MaxBlockSize))
	if r.writeBackWorkers > 0 {
		r.writeBackSemaphore = make(semaphore, r.writeBackWorkers)
	}

	return r, nil
}

// hashEncryptAndWriteMaybeAsync computes hash of a given buffer, optionally encrypts and writes it to storage.
// The write is not guaranteed to complete synchronously in case write-back is used, but by the time
// Repository.Close() returns all writes are guaranteed be over.
func (r *repository) hashEncryptAndWriteMaybeAsync(buffer *bytes.Buffer, prefix string) (ObjectID, error) {
	var data []byte
	if buffer != nil {
		data = buffer.Bytes()
	}

	var isAsync bool

	// Make sure we return buffer to the pool, but only if the request has not been asynchronous.
	defer func() {
		if !isAsync {
			r.bufferManager.returnBuffer(buffer)
		}
	}()

	if err := r.writeBackErrors.check(); err != nil {
		return NullObjectID, err
	}

	// Hash the block and compute encryption key.
	blockID, encryptionKey := r.formatter.ComputeBlockIDAndKey(data, r.format.Secret)
	atomic.AddInt32(&r.stats.HashedBlocks, 1)
	atomic.AddInt64(&r.stats.HashedBytes, int64(len(data)))

	objectID := ObjectID{
		StorageBlock:  prefix + blockID,
		EncryptionKey: encryptionKey,
	}

	// Before performing encryption, check if the block is already there.
	blockSize, err := r.storage.BlockSize(objectID.StorageBlock)
	if err == nil && blockSize == int64(len(data)) {
		// Block already exists in storage, correct size, return without uploading.
		return objectID, nil
	}

	if err != nil && err != storage.ErrBlockNotFound {
		// Don't know whether block exists in storage.
		return NullObjectID, err
	}

	// Encryption is requested, encrypt the block in-place.
	if encryptionKey != nil {
		data, err = r.formatter.Encrypt(data, encryptionKey)
		if err != nil {
			return NullObjectID, err
		}
	}

	if r.writeBackWorkers > 0 {
		// Tell the defer block not to return the buffer synchronously.
		isAsync = true

		r.waitGroup.Add(1)
		r.writeBackSemaphore.Lock()
		go func() {
			defer func() {
				r.bufferManager.returnBuffer(buffer)
				r.writeBackSemaphore.Unlock()
				r.waitGroup.Done()
			}()

			if err := r.storage.PutBlock(objectID.StorageBlock, data, storage.PutOptionsDefault); err != nil {
				r.writeBackErrors.add(err)
			}
		}()

		// async will fail later.
		return objectID, nil
	}

	// Synchronous case
	if err := r.storage.PutBlock(objectID.StorageBlock, data, storage.PutOptionsDefault); err != nil {
		return NullObjectID, err
	}

	return objectID, nil
}

func (r *repository) flattenListChunk(rawReader io.Reader) ([]indirectObjectEntry, error) {
	pr, err := jsonstream.NewReader(bufio.NewReader(rawReader), indirectStreamType)
	if err != nil {
		return nil, err
	}
	var seekTable []indirectObjectEntry

	for {
		var oe indirectObjectEntry

		err := pr.Read(&oe)
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Printf("Failed to read indirect object: %v", err)
			return nil, err
		}

		seekTable = append(seekTable, oe)
	}

	return seekTable, nil
}

func removeIndirection(o ObjectID) ObjectID {
	if o.Indirect <= 0 {
		panic("removeIndirection() called on a direct object")
	}
	o.Indirect--
	return o
}

func (r *repository) newRawReader(objectID ObjectID) (ObjectReader, error) {
	if objectID.Content != nil {
		return newObjectReaderWithData(objectID.Content), nil
	}

	blockID := objectID.StorageBlock
	payload, err := r.storage.GetBlock(blockID)
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&r.stats.BlocksReadFromStorage, 1)
	atomic.AddInt64(&r.stats.BytesReadFromStorage, int64(len(payload)))

	if len(objectID.EncryptionKey) > 0 {
		payload, err = r.formatter.Decrypt(payload, objectID.EncryptionKey)
		atomic.AddInt64(&r.stats.DecryptedBytes, int64(len(payload)))
		if err != nil {
			return nil, err
		}
	}

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	if err := r.verifyChecksum(payload, objectID.StorageBlock); err != nil {
		return nil, err
	}

	return newObjectReaderWithData(payload), nil
}

func (r *repository) verifyChecksum(data []byte, blockID string) error {
	expectedBlockID, _ := r.formatter.ComputeBlockIDAndKey(data, r.format.Secret)
	if !strings.HasSuffix(string(blockID), expectedBlockID) {
		atomic.AddInt32(&r.stats.InvalidBlocks, 1)
		return fmt.Errorf("invalid checksum for blob: '%v'", blockID)
	}

	atomic.AddInt32(&r.stats.ValidBlocks, 1)
	return nil
}

type readerWithData struct {
	io.ReadSeeker
	length int64
}

func (rwd *readerWithData) Close() error {
	return nil
}

func (rwd *readerWithData) Length() int64 {
	return rwd.length
}

func newObjectReaderWithData(data []byte) ObjectReader {
	return &readerWithData{
		ReadSeeker: bytes.NewReader(data),
		length:     int64(len(data)),
	}
}
