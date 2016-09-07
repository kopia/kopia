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

// ObjectReader allows reading, seeking, getting the length of and closing of a repository object.
type ObjectReader interface {
	io.Reader
	io.Seeker
	io.Closer
	Length() int64
}

type empty struct{}
type semaphore chan empty

func (s semaphore) Lock() {
	s <- empty{}
}

func (s semaphore) Unlock() {
	<-s
}

// Repository implements a content-addressable storage on top of blob storage.
type Repository struct {
	Stats   Stats           // vital statistics
	storage storage.Storage // underlying blob storage

	verbose       bool
	bufferManager *bufferManager
	format        Format
	formatter     ObjectFormatter

	writeBackWorkers   int
	writeBackSemaphore semaphore
	writeBackErrors    asyncErrors
	waitGroup          sync.WaitGroup
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

// Close closes the connection to the underlying blob storage and releases any resources.
func (r *Repository) Close() error {
	r.Flush()
	if err := r.storage.Close(); err != nil {
		return err
	}
	r.bufferManager.close()

	return nil
}

// Flush waits for all in-flight writes to complete.
func (r *Repository) Flush() error {
	if r.writeBackWorkers > 0 {
		r.waitGroup.Wait()
	}
	return nil
}

// NewWriter creates an ObjectWriter for writing to the repository.
func (r *Repository) NewWriter(options ...WriterOption) ObjectWriter {
	result := &objectWriter{
		repo:         r,
		blockTracker: &blockTracker{},
	}

	for _, option := range options {
		option(result)
	}

	return result
}

// Open creates new ObjectReader for reading given object from a repository.
func (r *Repository) Open(objectID ObjectID) (ObjectReader, error) {
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
type RepositoryOption func(o *Repository)

// WriteBack is an RepositoryOption that enables asynchronous writes to the storage using the pool
// of goroutines.
func WriteBack(writeBackWorkers int) RepositoryOption {
	return func(o *Repository) {
		o.writeBackWorkers = writeBackWorkers
	}
}

// EnableLogging is an RepositoryOption that causes all storage access to be logged.
func EnableLogging(options ...logging.Option) RepositoryOption {
	return func(o *Repository) {
		o.storage = logging.NewWrapper(o.storage, options...)
	}
}

// New creates a Repository with the specified storage, format and options.
func New(s storage.Storage, f *Format, options ...RepositoryOption) (*Repository, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}

	sf := SupportedFormats[f.ObjectFormat]

	r := &Repository{
		storage: s,
		format:  *f,
	}

	r.formatter = sf

	for _, o := range options {
		o(r)
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
func (r *Repository) hashEncryptAndWriteMaybeAsync(buffer *bytes.Buffer, prefix string) (ObjectID, error) {
	var data []byte
	if buffer != nil {
		data = buffer.Bytes()
	}

	if err := r.writeBackErrors.check(); err != nil {
		return NullObjectID, err
	}

	// Hash the block and compute encryption key.
	blockID, encryptionKey := r.formatter.ComputeBlockIDAndKey(data, r.format.Secret)
	atomic.AddInt32(&r.Stats.HashedBlocks, 1)
	atomic.AddInt64(&r.Stats.HashedBytes, int64(len(data)))

	objectID := ObjectID{
		StorageBlock:  prefix + blockID,
		EncryptionKey: encryptionKey,
	}

	if r.writeBackWorkers > 0 {
		// Tell the defer block not to return the buffer synchronously.
		r.waitGroup.Add(1)
		r.writeBackSemaphore.Lock()
		go func() {
			defer func() {
			}()

			if _, err := r.hashEncryptAndWrite(objectID, buffer, prefix); err != nil {
				r.writeBackErrors.add(err)
			}
			r.writeBackSemaphore.Unlock()
			r.waitGroup.Done()
		}()

		// async will fail later.
		return objectID, nil
	}

	return r.hashEncryptAndWrite(objectID, buffer, prefix)
}

func (r *Repository) hashEncryptAndWrite(objectID ObjectID, buffer *bytes.Buffer, prefix string) (ObjectID, error) {
	defer r.bufferManager.returnBuffer(buffer)

	var data []byte
	if buffer != nil {
		data = buffer.Bytes()
	}

	// Before performing encryption, check if the block is already there.
	blockSize, err := r.storage.BlockSize(objectID.StorageBlock)
	atomic.AddInt32(&r.Stats.CheckedBlocks, int32(1))
	if err == nil && blockSize == int64(len(data)) {
		atomic.AddInt32(&r.Stats.PresentBlocks, int32(1))
		// Block already exists in storage, correct size, return without uploading.
		return objectID, nil
	}

	if err != nil && err != storage.ErrBlockNotFound {
		// Don't know whether block exists in storage.
		return NullObjectID, err
	}

	// Encryption is requested, encrypt the block in-place.
	if objectID.EncryptionKey != nil {
		atomic.AddInt64(&r.Stats.EncryptedBytes, int64(len(data)))
		data, err = r.formatter.Encrypt(data, objectID.EncryptionKey)
		if err != nil {
			return NullObjectID, err
		}
	}

	atomic.AddInt32(&r.Stats.WrittenBlocks, int32(1))
	atomic.AddInt64(&r.Stats.WrittenBytes, int64(len(data)))

	if err := r.storage.PutBlock(objectID.StorageBlock, data, storage.PutOptionsDefault); err != nil {
		r.writeBackErrors.add(err)
	}

	return objectID, nil
}

func (r *Repository) flattenListChunk(rawReader io.Reader) ([]indirectObjectEntry, error) {
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

func (r *Repository) newRawReader(objectID ObjectID) (ObjectReader, error) {
	if objectID.BinaryContent != nil {
		return newObjectReaderWithData(objectID.BinaryContent), nil
	}

	if len(objectID.TextContent) > 0 {
		return newObjectReaderWithData([]byte(objectID.TextContent)), nil
	}

	blockID := objectID.StorageBlock
	payload, err := r.storage.GetBlock(blockID)
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&r.Stats.ReadBlocks, 1)
	atomic.AddInt64(&r.Stats.ReadBytes, int64(len(payload)))

	if len(objectID.EncryptionKey) > 0 {
		payload, err = r.formatter.Decrypt(payload, objectID.EncryptionKey)
		atomic.AddInt64(&r.Stats.DecryptedBytes, int64(len(payload)))
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

func (r *Repository) verifyChecksum(data []byte, blockID string) error {
	expectedBlockID, _ := r.formatter.ComputeBlockIDAndKey(data, r.format.Secret)
	if !strings.HasSuffix(string(blockID), expectedBlockID) {
		atomic.AddInt32(&r.Stats.InvalidBlocks, 1)
		return fmt.Errorf("invalid checksum for blob: '%v'", blockID)
	}

	atomic.AddInt32(&r.Stats.ValidBlocks, 1)
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
