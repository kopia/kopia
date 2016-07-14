package repo

import (
	"bufio"
	"bytes"
	"crypto/cipher"
	"crypto/hmac"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/kopia/kopia/storage"
	"github.com/kopia/kopia/storage/logging"
)

const (
	minLocatorSizeBytes = 16
)

// ObjectReader allows reading, seeking and closing of a repository object.
type ObjectReader interface {
	io.Reader
	io.Seeker
	io.Closer
}

// Since we never share keys, using constant IV is fine.
// Instead of using all-zero, we use this one.
var constantIV = []byte("kopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopia")

// Repository objects addressed by their content allows reading and writing them.
type Repository interface {
	// NewWriter opens an ObjectWriter for writing new content to the storage.
	NewWriter(options ...WriterOption) ObjectWriter

	// Open creates an io.ReadSeeker for reading object with a specified ID.
	Open(objectID ObjectID) (ObjectReader, error)

	Flush() error
	Storage() storage.Storage
	Close()

	ResetStats()
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

type keygenFunc func([]byte) (blockIDBytes []byte, key []byte)

type repository struct {
	storage       storage.Storage
	verbose       bool
	bufferManager *bufferManager
	stats         *RepositoryStats

	format   Format
	idFormat ObjectIDFormat
}

func (repo *repository) Close() {
	repo.Flush()
	repo.bufferManager.close()
}

func (repo *repository) Flush() error {
	if f, ok := repo.storage.(storage.Flusher); ok {
		return f.Flush()
	}

	return nil
}

func (repo *repository) ResetStats() {
	repo.stats = &RepositoryStats{}
}

func (repo *repository) Stats() RepositoryStats {
	return *repo.stats
}

func (repo *repository) Storage() storage.Storage {
	return repo.storage
}

func (repo *repository) NewWriter(options ...WriterOption) ObjectWriter {
	result := newObjectWriter(repo, ObjectIDTypeStored)

	for _, option := range options {
		option(result)
	}

	return result
}

func (repo *repository) Open(objectID ObjectID) (ObjectReader, error) {
	r, err := repo.newRawReader(objectID)
	if err != nil {
		return nil, err
	}

	if objectID.Type() == ObjectIDTypeList {
		seekTable := make([]seekTableEntry, 0, 100)

		seekTable, err = repo.flattenListChunk(seekTable, objectID, r)
		if err != nil {
			return nil, err
		}

		totalLength := seekTable[len(seekTable)-1].endOffset()

		return &objectReader{
			storage:     repo.storage,
			seekTable:   seekTable,
			totalLength: totalLength,
		}, nil
	}

	return r, err
}

// RepositoryOption controls the behavior of Repository.
type RepositoryOption func(o *repository) error

// WriteBack is an RepositoryOption that enables asynchronous writes to the storage using the pool
// of goroutines.
func WriteBack(workerCount int) RepositoryOption {
	return func(o *repository) error {
		o.storage = storage.NewWriteBackWrapper(o.storage, workerCount)
		return nil
	}
}

// EnableLogging is an RepositoryOption that causes all storage access to be logged.
func EnableLogging() RepositoryOption {
	return func(o *repository) error {
		o.storage = logging.NewWrapper(o.storage)
		return nil
	}
}

func hmacFunc(key []byte, hf func() hash.Hash) func() hash.Hash {
	return func() hash.Hash {
		return hmac.New(hf, key)
	}
}

// NewRepository creates new Repository with the specified storage, options, and key provider.
func NewRepository(
	r storage.Storage,
	f *Format,
	options ...RepositoryOption,
) (Repository, error) {
	if f.Version != "1" {
		return nil, fmt.Errorf("unsupported storage version: %v", f.Version)
	}
	if f.MaxBlobSize < 100 {
		return nil, fmt.Errorf("MaxBlobSize is not set")
	}

	sf := SupportedFormats.Find(f.ObjectFormat)
	if sf == nil {
		return nil, fmt.Errorf("unknown object format: %v", f.ObjectFormat)
	}

	repo := &repository{
		storage: r,
		format:  *f,
		stats:   &RepositoryStats{},
	}

	repo.idFormat = *sf

	for _, o := range options {
		if err := o(repo); err != nil {
			repo.Close()
			return nil, err
		}
	}

	repo.bufferManager = newBufferManager(repo.format.MaxBlobSize)

	return repo, nil
}

func (repo *repository) hashBuffer(data []byte) ([]byte, []byte) {
	return repo.idFormat.hashBuffer(data, repo.format.Secret)
}

func (repo *repository) hashBufferForWriting(buffer *bytes.Buffer, prefix string) (ObjectID, storage.ReaderWithLength, error) {
	var data []byte
	if buffer != nil {
		data = buffer.Bytes()
	}

	var blockCipher cipher.Block

	contentHash, cryptoKey := repo.hashBuffer(data)
	if cryptoKey != nil {
		var err error
		blockCipher, err = repo.idFormat.createCipher(cryptoKey)
		if err != nil {
			return "", nil, err
		}
	}

	var objectID ObjectID
	if len(cryptoKey) > 0 {
		objectID = ObjectID(prefix + hex.EncodeToString(contentHash) + objectIDEncryptionInfoSeparator + hex.EncodeToString(cryptoKey))
	} else {
		objectID = ObjectID(prefix + hex.EncodeToString(contentHash))
	}

	atomic.AddInt32(&repo.stats.HashedBlocks, 1)
	atomic.AddInt64(&repo.stats.HashedBytes, int64(len(data)))

	if buffer == nil {
		return objectID, storage.NewReader(bytes.NewBuffer(nil)), nil
	}

	blockReader := repo.bufferManager.returnBufferOnClose(buffer)
	blockReader = newCountingReader(blockReader, &repo.stats.BytesWrittenToStorage)

	if len(cryptoKey) > 0 {
		// Since we're not sharing the key, all-zero IV is ok.
		// We don't need to worry about separate MAC either, since hashing content produces object ID.
		ctr := cipher.NewCTR(blockCipher, constantIV[0:blockCipher.BlockSize()])

		blockReader = newCountingReader(
			newEncryptingReader(blockReader, ctr),
			&repo.stats.EncryptedBytes)
	}

	return objectID, blockReader, nil
}

func (repo *repository) flattenListChunk(
	seekTable []seekTableEntry,
	listObjectID ObjectID,
	rawReader io.Reader) ([]seekTableEntry, error) {

	scanner := bufio.NewScanner(rawReader)

	for scanner.Scan() {
		c := scanner.Text()
		comma := strings.Index(c, ",")
		if comma <= 0 {
			return nil, fmt.Errorf("unsupported entry '%v' in list '%s'", c, listObjectID)
		}

		length, err := strconv.ParseInt(c[0:comma], 10, 64)

		objectID, err := ParseObjectID(c[comma+1:])
		if err != nil {
			return nil, fmt.Errorf("unsupported entry '%v' in list '%s': %#v", c, listObjectID, err)
		}

		switch objectID.Type() {
		case ObjectIDTypeList:
			subreader, err := repo.newRawReader(objectID)
			if err != nil {
				return nil, err
			}

			seekTable, err = repo.flattenListChunk(seekTable, objectID, subreader)
			if err != nil {
				return nil, err
			}

		case ObjectIDTypeStored:
			var startOffset int64
			if len(seekTable) > 0 {
				startOffset = seekTable[len(seekTable)-1].endOffset()
			} else {
				startOffset = 0
			}

			seekTable = append(
				seekTable,
				seekTableEntry{
					blockID:     objectID.BlockID(),
					startOffset: startOffset,
					length:      length,
				})

		default:
			return nil, fmt.Errorf("unsupported entry '%v' in list '%v'", objectID, listObjectID)

		}
	}

	return seekTable, nil
}

func (repo *repository) newRawReader(objectID ObjectID) (ObjectReader, error) {
	inline := objectID.InlineData()
	if inline != nil {
		return newObjectReaderWithData(inline), nil
	}

	blockID := objectID.BlockID()
	payload, err := repo.storage.GetBlock(blockID)
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&repo.stats.BlocksReadFromStorage, 1)
	atomic.AddInt64(&repo.stats.BytesReadFromStorage, int64(len(payload)))

	if objectID.EncryptionInfo() == NoEncryption {
		if err := repo.verifyChecksum(payload, objectID.BlockID()); err != nil {
			return nil, err
		}
		return newObjectReaderWithData(payload), nil
	}

	cryptoKey, err := hex.DecodeString(string(objectID.EncryptionInfo()))
	if err != nil {
		return nil, errors.New("malformed encryption key")
	}

	blockCipher, err := repo.idFormat.createCipher(cryptoKey)
	if err != nil {
		return nil, errors.New("cannot create cipher")
	}

	iv := constantIV[0:blockCipher.BlockSize()]
	ctr := cipher.NewCTR(blockCipher, iv)
	ctr.XORKeyStream(payload, payload)

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	atomic.AddInt64(&repo.stats.DecryptedBytes, int64(len(payload)))

	if err := repo.verifyChecksum(payload, objectID.BlockID()); err != nil {
		return nil, err
	}

	return newObjectReaderWithData(payload), nil
}

func (repo *repository) verifyChecksum(data []byte, blockID string) error {
	payloadHash, _ := repo.hashBuffer(data)
	checksum := hex.EncodeToString(payloadHash)
	if !strings.HasSuffix(string(blockID), checksum) {
		atomic.AddInt32(&repo.stats.InvalidBlocks, 1)
		return fmt.Errorf("invalid checksum for blob: '%v'", blockID)
	}

	atomic.AddInt32(&repo.stats.ValidBlocks, 1)
	return nil
}

type readerWithData struct {
	io.ReadSeeker
}

func (rwd *readerWithData) Close() error {
	return nil
}

func newObjectReaderWithData(data []byte) ObjectReader {
	return &readerWithData{bytes.NewReader(data)}
}
