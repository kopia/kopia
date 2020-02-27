package object

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/splitter"
)

// Writer allows writing content to the storage and supports automatic deduplication and encryption
// of written data.
type Writer interface {
	io.WriteCloser

	Result() (ID, error)
}

type contentIDTracker struct {
	mu       sync.Mutex
	contents map[content.ID]bool
}

func (t *contentIDTracker) addContentID(contentID content.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.contents == nil {
		t.contents = make(map[content.ID]bool)
	}

	t.contents[contentID] = true
}

func (t *contentIDTracker) contentIDs() []content.ID {
	t.mu.Lock()
	defer t.mu.Unlock()

	result := make([]content.ID, 0, len(t.contents))
	for k := range t.contents {
		result = append(result, k)
	}

	return result
}

type objectWriter struct {
	ctx  context.Context
	repo *Manager

	compressor compression.Compressor

	prefix      content.ID
	buffer      bytes.Buffer
	totalLength int64

	currentPosition int64
	indirectIndex   []indirectObjectEntry

	description string

	splitter splitter.Splitter
}

func (w *objectWriter) Close() error {
	return nil
}

func (w *objectWriter) Write(data []byte) (n int, err error) {
	dataLen := len(data)
	w.totalLength += int64(dataLen)

	for _, d := range data {
		if err := w.buffer.WriteByte(d); err != nil {
			return 0, err
		}

		if w.splitter.ShouldSplit(d) {
			if err := w.flushBuffer(); err != nil {
				return 0, err
			}
		}
	}

	return dataLen, nil
}

func (w *objectWriter) flushBuffer() error {
	length := w.buffer.Len()
	chunkID := len(w.indirectIndex)
	w.indirectIndex = append(w.indirectIndex, indirectObjectEntry{})
	w.indirectIndex[chunkID].Start = w.currentPosition
	w.indirectIndex[chunkID].Length = int64(length)
	w.currentPosition += int64(length)

	contentBytes, isCompressed, err := maybeCompressedContentBytes(w.compressor, w.buffer.Bytes())
	if err != nil {
		return errors.Wrap(err, "unable to prepare content bytes")
	}

	w.buffer.Reset()

	contentID, err := w.repo.contentMgr.WriteContent(w.ctx, contentBytes, w.prefix)
	w.repo.trace("OBJECT_WRITER(%q) stored %v (%v bytes)", w.description, contentID, length)

	if err != nil {
		return errors.Wrapf(err, "error when flushing chunk %d of %s", chunkID, w.description)
	}

	oid := DirectObjectID(contentID)

	if isCompressed {
		oid = Compressed(oid)
	}

	w.indirectIndex[chunkID].Object = oid

	return nil
}

func maybeCompressedContentBytes(comp compression.Compressor, b []byte) (data []byte, isCompressed bool, err error) {
	if comp != nil {
		compressedBytes, err := comp.Compress(b)
		if err != nil {
			return nil, false, errors.Wrap(err, "compression error")
		}

		if len(compressedBytes) < len(b) {
			return compressedBytes, true, nil
		}
	}

	return append([]byte{}, b...), false, nil
}

func (w *objectWriter) Result() (ID, error) {
	if w.buffer.Len() > 0 || len(w.indirectIndex) == 0 {
		if err := w.flushBuffer(); err != nil {
			return "", err
		}
	}

	if len(w.indirectIndex) == 1 {
		return w.indirectIndex[0].Object, nil
	}

	iw := &objectWriter{
		ctx:         w.ctx,
		repo:        w.repo,
		compressor:  nil,
		description: "LIST(" + w.description + ")",
		splitter:    w.repo.newSplitter(),
		prefix:      w.prefix,
	}

	ind := indirectObject{
		StreamID: "kopia:indirect",
		Entries:  w.indirectIndex,
	}

	if err := json.NewEncoder(iw).Encode(ind); err != nil {
		return "", errors.Wrap(err, "unable to write indirect object index")
	}

	oid, err := iw.Result()
	if err != nil {
		return "", err
	}

	return IndirectObjectID(oid), nil
}

// WriterOptions can be passed to Repository.NewWriter()
type WriterOptions struct {
	Description string
	Prefix      content.ID // empty string or a single-character ('g'..'z')
	Compressor  compression.Name
}
