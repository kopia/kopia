// Package repodiag manages logs and metrics in the repository.
package repodiag

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/kopia/kopia/internal/blobcrypto"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/zaplogutil"
	"github.com/kopia/kopia/repo/blob"
)

const blobLoggerFlushThreshold = 4 << 20

// BlobPrefix is a prefix given to text logs stored in repository.
const BlobPrefix = "_log_"

// LogManager manages writing encrypted log blobs to the repository.
type LogManager struct {
	enabled atomic.Bool // set by enable(), logger is ineffective until called

	// InternalLogManager implements io.Writer and we must be able to write to the
	// repository asynchronously when the context is not provided.
	ctx context.Context //nolint:containedctx

	st             blob.Storage
	bc             blobcrypto.Crypter
	wg             sync.WaitGroup
	timeFunc       func() time.Time
	flushThreshold int
}

// Close closes the log manager.
func (m *LogManager) Close(ctx context.Context) {
	m.wg.Wait()
}

func (m *LogManager) encryptAndWriteLogBlob(prefix blob.ID, data gather.Bytes, closeFunc func()) {
	encrypted := gather.NewWriteBuffer()
	// Close happens in a goroutine

	blobID, err := blobcrypto.Encrypt(m.bc, data, prefix, "", encrypted)
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
func (m *LogManager) NewLogger() *zap.SugaredLogger {
	var rnd [2]byte

	rand.Read(rnd[:]) //nolint:errcheck

	w := &internalLogger{
		m:      m,
		prefix: blob.ID(fmt.Sprintf("%v%v_%x", BlobPrefix, clock.Now().Local().Format("20060102150405"), rnd)),
	}

	return zap.New(zapcore.NewCore(
		zaplogutil.NewStdConsoleEncoder(zaplogutil.StdConsoleEncoderConfig{
			TimeLayout: zaplogutil.PreciseLayout,
			LocalTime:  false,
		}),
		w, zap.DebugLevel), zap.WithClock(zaplogutil.Clock())).Sugar()
}

// Enable enables writing any buffered logs to repository.
func (m *LogManager) Enable() {
	if m == nil {
		return
	}

	m.enabled.Store(true)
}

// NewLogManager creates a new LogManager that will emit logs as repository blobs.
func NewLogManager(ctx context.Context, st blob.Storage, bc blobcrypto.Crypter) *LogManager {
	return &LogManager{
		ctx:            ctx,
		st:             st,
		bc:             bc,
		flushThreshold: blobLoggerFlushThreshold,
		timeFunc:       clock.Now,
	}
}
