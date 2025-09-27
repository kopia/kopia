// Package repodiag manages logs and metrics in the repository.
package repodiag

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// LogBlobPrefix is a prefix given to text logs stored in repository.
const LogBlobPrefix = "_log_"

// LogManager manages writing encrypted log blobs to the repository.
type LogManager struct {
	disableRepositoryLog bool

	// Set by Enable(). Log blobs are not written to the repository until
	// Enable() is called.
	enabled atomic.Bool

	// InternalLogManager implements io.Writer and we must be able to write to the
	// repository asynchronously when the context is not provided.
	ctx context.Context //nolint:containedctx

	writer *BlobWriter

	timeFunc       func() time.Time
	flushThreshold int // +checklocksignore
	prefix         blob.ID

	mu             sync.Mutex
	currentSegment *gather.WriteBuffer
	startTime      int64
	params         []contentlog.ParamWriter
	textWriter     io.Writer

	nextChunkNumber atomic.Uint64
	gz              *gzip.Writer
}

// NewLogger creates new logger.
func (m *LogManager) NewLogger(name string) *contentlog.Logger {
	if m == nil {
		return nil
	}

	return contentlog.NewLogger(
		m.outputEntry,
		append(append([]contentlog.ParamWriter(nil), m.params...), logparam.String("n", name))...)
}

// Enable enables writing log blobs to repository.
// Logs are not written to the repository until Enable is called.
func (m *LogManager) Enable() {
	if m == nil {
		return
	}

	m.enabled.Store(true)
}

// Disable disables writing log blobs to repository.
func (m *LogManager) Disable() {
	if m == nil {
		return
	}

	m.enabled.Store(false)
}

func (m *LogManager) outputEntry(data []byte) {
	if m.textWriter != nil {
		m.textWriter.Write(data) //nolint:errcheck
	}

	if !m.enabled.Load() || m.disableRepositoryLog {
		return
	}

	m.mu.Lock()

	var (
		flushBuffer *gather.WriteBuffer
		flushBlobID blob.ID
	)

	if m.currentSegment == nil || m.currentSegment.Length() > m.flushThreshold {
		flushBuffer, flushBlobID = m.initNewBuffer()
	}

	m.gz.Write(data) //nolint:errcheck
	m.mu.Unlock()

	if flushBuffer != nil {
		m.flushNextBuffer(flushBlobID, flushBuffer)
	}
}

func (m *LogManager) flushNextBuffer(blobID blob.ID, buf *gather.WriteBuffer) {
	m.writer.EncryptAndWriteBlobAsync(m.ctx, blobID, buf.Bytes(), buf.Close)
}

// Sync flushes the current buffer to the repository.
func (m *LogManager) Sync() {
	if m == nil {
		return
	}

	m.mu.Lock()
	flushBuffer, flushBlobID := m.initNewBuffer()
	m.mu.Unlock()

	if flushBuffer != nil {
		m.flushNextBuffer(flushBlobID, flushBuffer)
	}
}

func (m *LogManager) initNewBuffer() (flushBuffer *gather.WriteBuffer, flushBlobID blob.ID) {
	if m.gz != nil {
		m.gz.Close() //nolint:errcheck
	}

	flushBuffer = m.currentSegment

	if flushBuffer != nil {
		flushBlobID = blob.ID(fmt.Sprintf("%v_%v_%v_%v_", m.prefix, m.startTime, m.timeFunc().Unix(), m.nextChunkNumber.Add(1)))
	} else {
		flushBlobID = blob.ID("")
	}

	m.startTime = m.timeFunc().Unix()
	m.currentSegment = gather.NewWriteBuffer()
	m.gz = gzip.NewWriter(m.currentSegment)

	return flushBuffer, flushBlobID
}

// NewLogManager creates a new LogManager that will emit logs as repository blobs.
func NewLogManager(ctx context.Context, w *BlobWriter, disableRepositoryLog bool, textWriter io.Writer, params ...contentlog.ParamWriter) *LogManager {
	var rnd [2]byte

	rand.Read(rnd[:]) //nolint:errcheck

	return &LogManager{
		ctx:                  context.WithoutCancel(ctx),
		writer:               w,
		timeFunc:             clock.Now,
		params:               params,
		flushThreshold:       4 << 20, //nolint:mnd
		disableRepositoryLog: disableRepositoryLog,
		prefix:               blob.ID(fmt.Sprintf("%v%v_%x", LogBlobPrefix, clock.Now().Local().Format("20060102150405"), rnd)),
		textWriter:           textWriter,
	}
}
