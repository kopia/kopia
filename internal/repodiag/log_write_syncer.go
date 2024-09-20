package repodiag

import (
	"compress/gzip"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// logWriteSyncer writes a sequence of log messages as blobs in the repository.
type logWriteSyncer struct {
	nextChunkNumber atomic.Int32

	m  *LogManager
	mu sync.Mutex

	// +checklocks:mu
	buf *gather.WriteBuffer
	// +checklocks:mu
	gzw *gzip.Writer

	// +checklocks:mu
	startTime int64 // unix timestamp of the first log

	prefix blob.ID // +checklocksignore
}

func (l *logWriteSyncer) Write(b []byte) (int, error) {
	if l != nil {
		l.maybeEncryptAndWriteChunkUnlocked(l.addAndMaybeFlush(b))
	}

	return len(b), nil
}

func (l *logWriteSyncer) maybeEncryptAndWriteChunkUnlocked(data gather.Bytes, closeFunc func()) {
	if data.Length() == 0 {
		closeFunc()
		return
	}

	if !l.m.enabled.Load() {
		closeFunc()
		return
	}

	endTime := l.m.timeFunc().Unix()

	l.mu.Lock()
	st := l.startTime
	l.mu.Unlock()

	prefix := blob.ID(fmt.Sprintf("%v_%v_%v_%v_", l.prefix, st, endTime, l.nextChunkNumber.Add(1)))

	l.m.encryptAndWriteLogBlob(prefix, data, closeFunc)
}

func (l *logWriteSyncer) addAndMaybeFlush(b []byte) (payload gather.Bytes, closeFunc func()) {
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

// +checklocks:l.mu
func (l *logWriteSyncer) ensureWriterInitializedLocked() io.Writer {
	if l.gzw == nil {
		l.buf = gather.NewWriteBuffer()
		l.gzw = gzip.NewWriter(l.buf)
		l.startTime = l.m.timeFunc().Unix()
	}

	return l.gzw
}

// +checklocks:l.mu
func (l *logWriteSyncer) flushAndResetLocked() (payload gather.Bytes, closeFunc func()) {
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

func (l *logWriteSyncer) logUnexpectedError(err error) {
	if err == nil {
		return
	}
}

func (l *logWriteSyncer) Sync() error {
	l.mu.Lock()
	data, closeFunc := l.flushAndResetLocked()
	l.mu.Unlock()

	l.maybeEncryptAndWriteChunkUnlocked(data, closeFunc)

	return nil
}
