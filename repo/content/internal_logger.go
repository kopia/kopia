package content

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

const blobLoggerFlushThreshold = 4 << 20

// TextLogBlobPrefix is a prefix given to text logs stored in repositor.
const TextLogBlobPrefix = "_log_"

type internalLogManager struct {
	ctx            context.Context
	st             blob.Storage
	bc             *Crypter
	wg             sync.WaitGroup
	timeFunc       func() time.Time
	flushThreshold int
}

// Close closes the log manager.
func (m *internalLogManager) Close(ctx context.Context) {
	m.wg.Wait()
}

func (m *internalLogManager) encryptAndWriteLogBlob(prefix blob.ID, data gather.Bytes, closeFunc func()) {
	encrypted := gather.NewWriteBuffer()
	// Close happens in a goroutine

	blobID, err := m.bc.EncryptBLOB(data, prefix, "", encrypted)
	if err != nil {
		encrypted.Close()

		// this should not happen, also nothing can be done about this, we're not in a place where we can return error, log it.
		return
	}

	b := encrypted.Bytes()

	m.wg.Add(1)

	go func() {
		defer m.wg.Done()
		defer encrypted.Close()
		defer closeFunc()

		if err := m.st.PutBlob(m.ctx, blobID, b); err != nil {
			// nothing can be done about this, we're not in a place where we can return error, log it.
			return
		}
	}()
}

// NewLogger creates new logger.
func (m *internalLogManager) NewLogger() *internalLogger {
	var rnd [2]byte

	rand.Read(rnd[:]) // nolint:errcheck

	return &internalLogger{
		m:      m,
		prefix: blob.ID(fmt.Sprintf("%v%v_%x", TextLogBlobPrefix, clock.Now().Local().Format("20060102150405"), rnd)),
	}
}

// internalLogger represents a single log session that saves log files as blobs in the repository.
// The logger starts disabled and to actually persist logs enable() must be called.
type internalLogger struct {
	nextChunkNumber int32 // chunk number incremented using atomic.AddInt32()
	enabled         int32 // set by enable(), logger is ineffective until called

	m         *internalLogManager
	mu        sync.Mutex
	buf       *gather.WriteBuffer
	gzw       *gzip.Writer
	startTime int64 // unix timestamp of the first log
	prefix    blob.ID
}

func (l *internalLogger) enable() {
	if l == nil {
		return
	}

	atomic.StoreInt32(&l.enabled, 1)
}

// Close closes the log session and saves any pending log.
func (l *internalLogger) Close(ctx context.Context) {
	l.mu.Lock()
	data, closeFunc := l.flushAndResetLocked()
	l.mu.Unlock()

	l.maybeEncryptAndWriteChunkUnlocked(data, closeFunc)
}

func (l *internalLogger) nowString() string {
	return l.m.timeFunc().UTC().Format("2006-01-02T15:04:05.000000Z")
}

func (l *internalLogger) add(level, msg string, args []interface{}) {
	prefix := l.nowString() + " " + level + " "
	line := strings.TrimSpace(fmt.Sprintf(prefix+msg, args...)) + "\n"

	l.maybeEncryptAndWriteChunkUnlocked(l.addLineAndMaybeFlush(line))
}

func (l *internalLogger) maybeEncryptAndWriteChunkUnlocked(data gather.Bytes, closeFunc func()) {
	if data.Length() == 0 {
		return
	}

	if atomic.LoadInt32(&l.enabled) == 0 {
		return
	}

	endTime := l.m.timeFunc().Unix()

	l.mu.Lock()
	prefix := blob.ID(fmt.Sprintf("%v_%v_%v_%v_", l.prefix, l.startTime, endTime, atomic.AddInt32(&l.nextChunkNumber, 1)))
	l.mu.Unlock()

	l.m.encryptAndWriteLogBlob(prefix, data, closeFunc)
}

func (l *internalLogger) addLineAndMaybeFlush(line string) (payload gather.Bytes, closeFunc func()) {
	l.mu.Lock()
	defer l.mu.Unlock()

	w := l.ensureWriterInitializedLocked()

	_, err := io.WriteString(w, line)
	l.logUnexpectedError(err)

	if l.buf.Length() < l.m.flushThreshold {
		return gather.Bytes{}, func() {}
	}

	return l.flushAndResetLocked()
}

func (l *internalLogger) ensureWriterInitializedLocked() io.Writer {
	if l.gzw == nil {
		l.buf = gather.NewWriteBuffer()
		l.gzw = gzip.NewWriter(l.buf)
		l.startTime = l.m.timeFunc().Unix()
	}

	return l.gzw
}

func (l *internalLogger) flushAndResetLocked() (payload gather.Bytes, closeFunc func()) {
	if l.gzw == nil {
		return gather.Bytes{}, func() {}
	}

	l.logUnexpectedError(l.gzw.Flush())
	l.logUnexpectedError(l.gzw.Close())

	closeBuf := l.buf.Close
	res := l.buf.Bytes()

	l.buf = nil
	l.gzw = nil

	return res, closeBuf
}

func (l *internalLogger) logUnexpectedError(err error) {
	if err == nil {
		return
	}
}

func (l *internalLogger) Debugf(msg string, args ...interface{}) {
	l.add("DEBUG", msg, args)
}

func (l *internalLogger) Infof(msg string, args ...interface{}) {
	l.add("INFO", msg, args)
}

func (l *internalLogger) Errorf(msg string, args ...interface{}) {
	l.add("ERROR", msg, args)
}

// newInternalLogManager creates a new blobLogManager that will emit logs as repository blobs with a given prefix.
func newInternalLogManager(ctx context.Context, st blob.Storage, bc *Crypter) *internalLogManager {
	return &internalLogManager{
		ctx:            ctx,
		st:             st,
		bc:             bc,
		flushThreshold: blobLoggerFlushThreshold,
		timeFunc:       clock.Now,
	}
}
