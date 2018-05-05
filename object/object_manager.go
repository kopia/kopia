package object

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/internal/jsonstream"
)

// Reader allows reading, seeking, getting the length of and closing of a repository object.
type Reader interface {
	io.Reader
	io.Seeker
	io.Closer
	Length() int64
}

type blockManager interface {
	BlockInfo(ctx context.Context, blockID string) (block.Info, error)
	GetBlock(ctx context.Context, blockID string) ([]byte, error)
	WriteBlock(ctx context.Context, data []byte, prefix string) (string, error)
	Flush(ctx context.Context) error
}

// Manager implements a content-addressable storage on top of blob storage.
type Manager struct {
	Format config.RepositoryObjectFormat

	blockMgr blockManager

	async              bool
	writeBackWG        sync.WaitGroup
	writeBackSemaphore semaphore

	trace func(message string, args ...interface{})

	newSplitter func() objectSplitter
}

// Close closes the connection to the underlying blob storage and releases any resources.
func (om *Manager) Close(ctx context.Context) error {
	om.writeBackWG.Wait()
	return om.Flush(ctx)
}

// NewWriter creates an ObjectWriter for writing to the repository.
func (om *Manager) NewWriter(ctx context.Context, opt WriterOptions) Writer {
	w := &objectWriter{
		ctx:         ctx,
		repo:        om,
		splitter:    om.newSplitter(),
		description: opt.Description,
	}

	if opt.splitter != nil {
		w.splitter = opt.splitter
	}

	return w
}

// Open creates new ObjectReader for reading given object from a repository.
func (om *Manager) Open(ctx context.Context, objectID ID) (Reader, error) {
	// log.Printf("Repository::Open %v", objectID.String())
	// defer log.Printf("finished Repository::Open() %v", objectID.String())

	// Flush any pending writes.
	om.writeBackWG.Wait()

	if objectID.Indirect != nil {
		rd, err := om.Open(ctx, *objectID.Indirect)
		if err != nil {
			return nil, err
		}
		defer rd.Close() //nolint:errcheck

		seekTable, err := om.flattenListChunk(rd)
		if err != nil {
			return nil, err
		}

		totalLength := seekTable[len(seekTable)-1].endOffset()

		return &objectReader{
			ctx:         ctx,
			repo:        om,
			seekTable:   seekTable,
			totalLength: totalLength,
		}, nil
	}

	return om.newRawReader(ctx, objectID)
}

// VerifyObject ensures that all objects backing ObjectID are present in the repository
// and returns the total length of the object and storage blocks of which it is composed.
func (om *Manager) VerifyObject(ctx context.Context, oid ID) (int64, []string, error) {
	// Flush any pending writes.
	om.writeBackWG.Wait()

	blocks := &blockTracker{}
	l, err := om.verifyObjectInternal(ctx, oid, blocks)
	if err != nil {
		return 0, nil, err
	}

	return l, blocks.blockIDs(), nil
}

func (om *Manager) verifyIndirectObjectInternal(ctx context.Context, oid ID, blocks *blockTracker) (int64, error) {
	if _, err := om.verifyObjectInternal(ctx, *oid.Indirect, blocks); err != nil {
		return 0, fmt.Errorf("unable to read index: %v", err)
	}
	rd, err := om.Open(ctx, *oid.Indirect)
	if err != nil {
		return 0, err
	}
	defer rd.Close() //nolint:errcheck

	seekTable, err := om.flattenListChunk(rd)
	if err != nil {
		return 0, err
	}

	for i, m := range seekTable {
		l, err := om.verifyObjectInternal(ctx, m.Object, blocks)
		if err != nil {
			return 0, err
		}

		if l != m.Length {
			return 0, fmt.Errorf("unexpected length of part %#v of indirect object %q: %v %v, expected %v", i, oid, m.Object, l, m.Length)
		}
	}

	totalLength := seekTable[len(seekTable)-1].endOffset()
	return totalLength, nil
}

func (om *Manager) verifyObjectInternal(ctx context.Context, oid ID, blocks *blockTracker) (int64, error) {
	if oid.Indirect != nil {
		return om.verifyIndirectObjectInternal(ctx, oid, blocks)
	}

	p, err := om.blockMgr.BlockInfo(ctx, oid.ContentBlockID)
	if err != nil {
		return 0, err
	}
	blocks.addBlock(oid.ContentBlockID)

	if p.PackBlockID != "" {
		l, err := om.verifyObjectInternal(ctx, ID{ContentBlockID: string(p.PackBlockID)}, blocks)
		if err != nil {
			return 0, err
		}

		if int64(p.PackOffset+p.Length) <= l {
			return int64(p.Length), nil
		}

		return 0, fmt.Errorf("packed object %v does not fit within its parent pack %v (pack length %v)", oid, p, l)
	}

	return int64(p.Length), nil
}

// Flush closes any pending pack files. Once this method returns, ObjectIDs returned by ObjectManager are
// ok to be used.
func (om *Manager) Flush(ctx context.Context) error {
	om.writeBackWG.Wait()
	return nil
}

func nullTrace(message string, args ...interface{}) {
}

// validateFormat checks the validity of RepositoryObjectFormat and returns an error if invalid.
func validateFormat(f *config.RepositoryObjectFormat) error {
	if f.Version != 1 {
		return fmt.Errorf("unsupported version: %v", f.Version)
	}

	return nil
}

// ManagerOptions specifies object manager options.
type ManagerOptions struct {
	WriteBack int
	Trace     func(message string, args ...interface{})
}

// NewObjectManager creates an ObjectManager with the specified block manager and format.
func NewObjectManager(ctx context.Context, bm blockManager, f config.RepositoryObjectFormat, opts ManagerOptions) (*Manager, error) {
	if err := validateFormat(&f); err != nil {
		return nil, err
	}

	om := &Manager{
		blockMgr: bm,
		Format:   f,
		trace:    nullTrace,
	}

	splitterID := f.Splitter
	if splitterID == "" {
		splitterID = "FIXED"
	}

	os := splitterFactories[splitterID]
	if os == nil {
		return nil, fmt.Errorf("unsupported splitter %q", f.Splitter)
	}

	om.newSplitter = func() objectSplitter {
		return os(&f)
	}

	if opts.Trace != nil {
		om.trace = opts.Trace
	} else {
		om.trace = nullTrace
	}

	if opts.WriteBack > 0 {
		om.async = true
		om.writeBackSemaphore = make(semaphore, opts.WriteBack)
	}

	return om, nil
}

func (om *Manager) flattenListChunk(rawReader io.Reader) ([]indirectObjectEntry, error) {
	pr, err := jsonstream.NewReader(bufio.NewReader(rawReader), indirectStreamType, nil)
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

func (om *Manager) newRawReader(ctx context.Context, objectID ID) (Reader, error) {
	payload, err := om.blockMgr.GetBlock(ctx, objectID.ContentBlockID)
	if err != nil {
		return nil, err
	}

	return newObjectReaderWithData(payload), nil
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

func newObjectReaderWithData(data []byte) Reader {
	return &readerWithData{
		ReadSeeker: bytes.NewReader(data),
		length:     int64(len(data)),
	}
}
