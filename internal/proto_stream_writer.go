package internal

import (
	"encoding/binary"
	"io"

	"github.com/golang/protobuf/proto"
)

// ProtoStreamWriter writes a stream of length-prefix proto messages with an optional header.
type ProtoStreamWriter struct {
	writer          io.Writer
	buf             *proto.Buffer
	lengthPrefixBuf []byte
	header          []byte
}

// Write emits length-prefixed proto message to its output.
func (w *ProtoStreamWriter) Write(v proto.Message) error {
	if w.header != nil {
		if _, err := w.writer.Write(w.header); err != nil {
			return err
		}
		w.header = nil
	}

	w.buf.Reset()
	if err := w.buf.Marshal(v); err != nil {
		return err
	}

	protoBytes := w.buf.Bytes()

	if w.lengthPrefixBuf == nil {
		w.lengthPrefixBuf = make([]byte, binary.MaxVarintLen64)
	}

	lengthBytes := w.lengthPrefixBuf[0:binary.PutUvarint(w.lengthPrefixBuf[:], uint64(len(protoBytes)))]

	if _, err := w.writer.Write(lengthBytes); err != nil {
		return err
	}

	if _, err := w.writer.Write(protoBytes); err != nil {
		return err
	}

	return nil
}

// NewProtoStreamWriter creates a new ProtoStreamWriter on top of a specified writer with a specified optional header.
func NewProtoStreamWriter(w io.Writer, header []byte) *ProtoStreamWriter {
	return &ProtoStreamWriter{
		writer: w,
		buf:    proto.NewBuffer(nil),
		header: header,
	}
}
