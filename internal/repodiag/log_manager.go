// Package repodiag manages logs and metrics in the repository.
package repodiag

import (
	"context"
	"crypto/rand"
	"fmt"
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

// LogBlobPrefix is a prefix given to text logs stored in repository.
const LogBlobPrefix = "_log_"

// LogManager manages writing encrypted log blobs to the repository.
type LogManager struct {
	// Set by Enable(). Log blobs are not written to the repository until
	// Enable() is called.
	enabled atomic.Bool

	// InternalLogManager implements io.Writer and we must be able to write to the
	// repository asynchronously when the context is not provided.
	ctx context.Context //nolint:containedctx

	writer *BlobWriter

	timeFunc       func() time.Time
	flushThreshold int
}

func (m *LogManager) encryptAndWriteLogBlob(prefix blob.ID, data gather.Bytes, closeFunc func()) {
	m.writer.EncryptAndWriteBlobAsync(m.ctx, prefix, data, closeFunc)
}

// NewLogger creates new logger.
func (m *LogManager) NewLogger() *zap.SugaredLogger {
	if m == nil {
		return zap.NewNop().Sugar()
	}

	var rnd [2]byte

	rand.Read(rnd[:]) //nolint:errcheck

	w := &logWriteSyncer{
		m:      m,
		prefix: blob.ID(fmt.Sprintf("%v%v_%x", LogBlobPrefix, clock.Now().Local().Format("20060102150405"), rnd)),
	}

	return zap.New(zapcore.NewCore(
		zaplogutil.NewStdConsoleEncoder(zaplogutil.StdConsoleEncoderConfig{
			TimeLayout: zaplogutil.PreciseLayout,
			LocalTime:  false,
		}),
		w, zap.DebugLevel), zap.WithClock(zaplogutil.Clock())).Sugar()
}

// Enable enables writing log blobs to repository.
// Logs are not written to the repository until Enable is called.
func (m *LogManager) Enable() {
	if m == nil {
		return
	}

	m.enabled.Store(true)
}

// NewLogManager creates a new LogManager that will emit logs as repository blobs.
func NewLogManager(ctx context.Context, w *BlobWriter) *LogManager {
	return &LogManager{
		ctx:            ctx,
		writer:         w,
		flushThreshold: blobLoggerFlushThreshold,
		timeFunc:       clock.Now,
	}
}
