package content

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/zaplogutil"
	"github.com/kopia/kopia/repo/blob"
)

const blobLoggerFlushThreshold = 4 << 20

// TextLogBlobPrefix is a prefix given to text logs stored in repositor.
const TextLogBlobPrefix = "_log_"

type internalLogManager struct {
	enabled int32 // set by enable(), logger is ineffective until called

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

		if err := m.st.PutBlob(m.ctx, blobID, b, blob.PutOptions{}); err != nil {
			// nothing can be done about this, we're not in a place where we can return error, log it.
			return
		}
	}()
}

// NewLogger creates new logger.
func (m *internalLogManager) NewLogger() *zap.SugaredLogger {
	var rnd [2]byte

	rand.Read(rnd[:]) // nolint:errcheck

	w := &internalLogger{
		m:      m,
		prefix: blob.ID(fmt.Sprintf("%v%v_%x", TextLogBlobPrefix, clock.Now().Local().Format("20060102150405"), rnd)),
	}

	return zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
			TimeKey:          "t",
			MessageKey:       "m",
			NameKey:          "n",
			EncodeTime:       zaplogutil.TimezoneAdjust(zaplogutil.PreciseTimeEncoder, false),
			EncodeDuration:   zapcore.StringDurationEncoder,
			ConsoleSeparator: " ",
		}),
		w, zap.DebugLevel), zap.WithClock(zaplogutil.Clock)).Sugar()
}

// internalLogger represents a single log session that saves log files as blobs in the repository.
// The logger starts disabled and to actually persist logs enable() must be called.
type internalLogger struct {
	nextChunkNumber int32 // chunk number incremented using atomic.AddInt32()

	m         *internalLogManager
	mu        sync.Mutex
	buf       *gather.WriteBuffer
	gzw       *gzip.Writer
	startTime int64 // unix timestamp of the first log
	prefix    blob.ID
}

func (m *internalLogManager) enable() {
	if m == nil {
		return
	}

	atomic.StoreInt32(&m.enabled, 1)
}

func (l *internalLogger) Write(b []byte) (int, error) {
	l.maybeEncryptAndWriteChunkUnlocked(l.addAndMaybeFlush(b))
	return len(b), nil
}

func (l *internalLogger) maybeEncryptAndWriteChunkUnlocked(data gather.Bytes, closeFunc func()) {
	if data.Length() == 0 {
		closeFunc()
		return
	}

	if atomic.LoadInt32(&l.m.enabled) == 0 {
		closeFunc()
		return
	}

	endTime := l.m.timeFunc().Unix()

	l.mu.Lock()
	prefix := blob.ID(fmt.Sprintf("%v_%v_%v_%v_", l.prefix, l.startTime, endTime, atomic.AddInt32(&l.nextChunkNumber, 1)))
	l.mu.Unlock()

	l.m.encryptAndWriteLogBlob(prefix, data, closeFunc)
}

func (l *internalLogger) addAndMaybeFlush(b []byte) (payload gather.Bytes, closeFunc func()) {
	l.mu.Lock()
	defer l.mu.Unlock()

	w := l.ensureWriterInitializedLocked()

	_, err := w.Write(b)
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

func (l *internalLogger) Sync() error {
	l.mu.Lock()
	data, closeFunc := l.flushAndResetLocked()
	l.mu.Unlock()

	l.maybeEncryptAndWriteChunkUnlocked(data, closeFunc)

	return nil
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
