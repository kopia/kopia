package repo

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"
	"sync/atomic"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/blob/logging"
	"github.com/kopia/kopia/internal/jsonstream"
)

// ObjectReader allows reading, seeking, getting the length of and closing of a repository object.
type ObjectReader interface {
	io.Reader
	io.Seeker
	io.Closer
	Length() int64
}

// Repository implements a content-addressable storage on top of blob storage.
type Repository struct {
	Stats   Stats        // vital statistics
	Storage blob.Storage // underlying blob storage

	verbose       bool
	bufferManager *bufferManager
	format        Format
	formatter     ObjectFormatter

	writeBack writebackManager

	newSplitter func() objectSplitter
}

// Close closes the connection to the underlying blob storage and releases any resources.
func (r *Repository) Close() error {
	r.Flush()
	if err := r.Storage.Close(); err != nil {
		return err
	}
	r.bufferManager.close()

	return nil
}

// Flush waits for all in-flight writes to complete.
func (r *Repository) Flush() error {
	r.writeBack.flush()
	return nil
}

// NewWriter creates an ObjectWriter for writing to the repository.
func (r *Repository) NewWriter(options ...WriterOption) ObjectWriter {
	result := &objectWriter{
		repo:         r,
		blockTracker: &blockTracker{},
		splitter:     r.newSplitter(),
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
		defer rd.Close()

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
		o.writeBack.workers = writeBackWorkers
	}
}

// EnableLogging is an RepositoryOption that causes all storage access to be logged.
func EnableLogging(options ...logging.Option) RepositoryOption {
	return func(o *Repository) {
		o.Storage = logging.NewWrapper(o.Storage, options...)
	}
}

// New creates a Repository with the specified storage, format and options.
func New(s blob.Storage, f *Format, options ...RepositoryOption) (*Repository, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}

	sf := SupportedFormats[f.ObjectFormat]

	r := &Repository{
		Storage: s,
		format:  *f,
	}

	sp := f.Splitter
	if sp == "" {
		sp = "FIXED"
	}

	os := SupportedSplitters[sp]
	if os == nil {
		return nil, fmt.Errorf("unsupported splitter %q", sp)
	}

	r.newSplitter = func() objectSplitter {
		return os(f)
	}

	var err error

	r.formatter, err = sf(f)
	if err != nil {
		return nil, err
	}

	for _, o := range options {
		o(r)
	}

	r.bufferManager = newBufferManager(int(r.format.MaxBlockSize))
	if r.writeBack.enabled() {
		r.writeBack.semaphore = make(semaphore, r.writeBack.workers)
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

	if err := r.writeBack.errors.check(); err != nil {
		return NullObjectID, err
	}

	// Hash the block and compute encryption key.
	objectID := r.formatter.ComputeObjectID(data)
	objectID.StorageBlock = prefix + objectID.StorageBlock
	atomic.AddInt32(&r.Stats.HashedBlocks, 1)
	atomic.AddInt64(&r.Stats.HashedBytes, int64(len(data)))

	if r.writeBack.enabled() {
		// Tell the defer block not to return the buffer synchronously.
		r.writeBack.waitGroup.Add(1)
		r.writeBack.semaphore.Lock()
		go func() {
			if _, err := r.encryptAndMaybeWrite(objectID, buffer, prefix); err != nil {
				r.writeBack.errors.add(err)
			}
			r.writeBack.semaphore.Unlock()
			r.writeBack.waitGroup.Done()
		}()

		// async will fail later.
		return objectID, nil
	}

	return r.encryptAndMaybeWrite(objectID, buffer, prefix)
}

func (r *Repository) encryptAndMaybeWrite(objectID ObjectID, buffer *bytes.Buffer, prefix string) (ObjectID, error) {
	defer r.bufferManager.returnBuffer(buffer)

	var data []byte
	if buffer != nil {
		data = buffer.Bytes()
	}

	// Before performing encryption, check if the block is already there.
	blockSize, err := r.Storage.BlockSize(objectID.StorageBlock)
	atomic.AddInt32(&r.Stats.CheckedBlocks, int32(1))
	if err == nil && blockSize == int64(len(data)) {
		atomic.AddInt32(&r.Stats.PresentBlocks, int32(1))
		// Block already exists in storage, correct size, return without uploading.
		return objectID, nil
	}

	if err != nil && err != blob.ErrBlockNotFound {
		// Don't know whether block exists in storage.
		return NullObjectID, err
	}

	// Encrypt the block in-place.
	atomic.AddInt64(&r.Stats.EncryptedBytes, int64(len(data)))
	data, err = r.formatter.Encrypt(data, objectID)
	if err != nil {
		return NullObjectID, err
	}

	atomic.AddInt32(&r.Stats.WrittenBlocks, int32(1))
	atomic.AddInt64(&r.Stats.WrittenBytes, int64(len(data)))

	if err := r.Storage.PutBlock(objectID.StorageBlock, data, blob.PutOptionsDefault); err != nil {
		r.writeBack.errors.add(err)
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
	payload, err := r.Storage.GetBlock(blockID)
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&r.Stats.ReadBlocks, 1)
	atomic.AddInt64(&r.Stats.ReadBytes, int64(len(payload)))

	payload, err = r.formatter.Decrypt(payload, objectID)
	atomic.AddInt64(&r.Stats.DecryptedBytes, int64(len(payload)))
	if err != nil {
		return nil, err
	}

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	if err := r.verifyChecksum(payload, objectID.StorageBlock); err != nil {
		return nil, err
	}

	return newObjectReaderWithData(payload), nil
}

func (r *Repository) verifyChecksum(data []byte, blockID string) error {
	expected := r.formatter.ComputeObjectID(data)
	if !strings.HasSuffix(blockID, expected.StorageBlock) {
		atomic.AddInt32(&r.Stats.InvalidBlocks, 1)
		return fmt.Errorf("invalid checksum for blob: '%v', expected %v", blockID, expected.StorageBlock)
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
