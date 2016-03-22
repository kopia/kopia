package cas

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/kopia/kopia/content"
	"github.com/kopia/kopia/storage"
)

type blockHasher interface {
	hashBuffer(data []byte) string
}

// ObjectWriter allows writing content to the repository and supports automatic deduplication and encryption
// of written data.
type ObjectWriter interface {
	io.WriteCloser

	Result(forceStored bool) (content.ObjectID, error)
}

// objectWriterConfig
type objectWriterConfig struct {
	mgr        *objectManager
	putOptions storage.PutOptions
}

type objectWriter struct {
	objectWriterConfig

	buffer      *bytes.Buffer
	totalLength int64

	prefix             string
	listWriter         *objectWriter
	flushedObjectCount int
	lastFlushedObject  content.ObjectID

	description string
	objectType  content.ObjectIDType

	atomicWrites bool
}

func (w *objectWriter) Close() error {
	if w.buffer != nil {
		w.mgr.bufferManager.returnBuffer(w.buffer)
		w.buffer = nil
	}
	if w.listWriter != nil {
		w.listWriter.Close()
		w.listWriter = nil
	}
	return nil
}

func (w *objectWriter) Write(data []byte) (n int, err error) {
	remaining := len(data)
	w.totalLength += int64(remaining)

	for remaining > 0 {
		if w.buffer == nil {
			w.buffer = w.mgr.bufferManager.newBuffer()
		}
		room := w.buffer.Cap() - w.buffer.Len()

		if remaining <= room {
			w.buffer.Write(data)
			remaining = 0
		} else {
			if w.atomicWrites {
				if w.buffer == nil {
					// We're at the beginning of a buffer, fail if the buffer is too small.
					return 0, fmt.Errorf("object writer buffer too small, need: %v, have: %v", remaining, room)
				}
				if err := w.flushBuffer(); err != nil {
					return 0, err
				}

				continue
			}

			w.buffer.Write(data[0:room])

			if err := w.flushBuffer(); err != nil {
				return 0, err
			}
			data = data[room:]
			remaining = len(data)
		}
	}
	return len(data), nil
}

func (w *objectWriter) flushBuffer() error {
	if w.buffer != nil {
		data := w.buffer.Bytes()
		length := w.buffer.Len()

		b := w.mgr.bufferManager.returnBufferOnClose(w.buffer)
		w.buffer = nil

		objectID, transformer := w.mgr.formatter.Do(data, string(w.objectType)+w.prefix)
		b = transformer(b)

		if err := w.mgr.repository.PutBlock(objectID.BlockID(), b, storage.PutOptions{}); err != nil {
			return fmt.Errorf(
				"error when flushing chunk %d of %s to %#v: %#v",
				w.flushedObjectCount,
				w.description,
				objectID.BlockID(),
				err)
		}

		w.flushedObjectCount++
		w.lastFlushedObject = objectID
		if w.listWriter == nil {
			w.listWriter = newObjectWriter(w.objectWriterConfig, content.ObjectIDTypeList)
			w.listWriter.description = "LIST(" + w.description + ")"
			w.listWriter.atomicWrites = true
		}

		fmt.Fprintf(w.listWriter, "%v,%v\n", length, objectID)
	}
	return nil
}

func newObjectWriter(cfg objectWriterConfig, objectType content.ObjectIDType) *objectWriter {
	return &objectWriter{
		objectWriterConfig: cfg,
		objectType:         objectType,
	}
}

func (w *objectWriter) Result(forceStored bool) (content.ObjectID, error) {
	if !forceStored && w.flushedObjectCount == 0 {
		if w.buffer == nil {
			return content.NewInlineBinaryObjectID(nil), nil
		}

		if w.buffer.Len() < w.mgr.maxInlineBlobSize {
			data := w.buffer.Bytes()
			if !utf8.Valid(data) {
				return content.NewInlineBinaryObjectID(data), nil
			}

			// If the binary represents valid UTF8, try encoding it as text (JSON) or binary chunk
			// and pick the one that has shorter representation.
			dataString := string(data)
			jsonData, _ := json.Marshal(dataString)

			jsonLen := len(jsonData)
			base64Len := base64.StdEncoding.EncodedLen(len(data))

			if jsonLen < base64Len {
				return content.NewInlineTextObjectID(dataString), nil
			}

			return content.NewInlineBinaryObjectID(data), nil
		}
	}

	w.flushBuffer()
	defer func() {
		w.listWriter.Close()
	}()

	if w.flushedObjectCount == 1 {
		return w.lastFlushedObject, nil
	} else if w.flushedObjectCount == 0 {
		return content.NullObjectID, nil
	} else {
		return w.listWriter.Result(true)
	}
}

// WriterOption is an option that can be passed to ObjectManager.NewWriter()
type WriterOption func(*objectWriter)

// WithBlockNamePrefix causes the ObjectWriter to prefix any blocks emitted to the repository with a given string.
func WithBlockNamePrefix(prefix string) WriterOption {
	return func(w *objectWriter) {
		w.prefix = prefix
	}
}

// WithDescription is used for debugging only and causes the following string to be emitted in logs.
func WithDescription(description string) WriterOption {
	return func(w *objectWriter) {
		w.description = description
	}
}

// WithPutOptions causes the ObjectWriter to use the specified options when writing blocks to the repository.
func WithPutOptions(options storage.PutOptions) WriterOption {
	return func(w *objectWriter) {
		w.putOptions = options
	}
}
