package internal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
)

// ProtoStreamReader reads a stream of length-prefix proto messages with an optional header.
type ProtoStreamReader struct {
	reader  *bufio.Reader
	readBuf []byte
	buf     *proto.Buffer
	header  []byte
}

// Read reads the next proto message from the stream, returns io.EOF on the end of stream.
func (r *ProtoStreamReader) Read(v proto.Message) error {
	v.Reset()
	if r.header != nil {
		p := make([]byte, len(r.header))
		if _, err := r.reader.Read(p); err == io.EOF {
			return io.EOF
		}

		if !bytes.Equal(p, r.header) {
			return fmt.Errorf("invalid stream header: %v, expected %v", string(p), string(r.header))
		}

		r.header = nil

	}
	length64, err := binary.ReadUvarint(r.reader)
	if err != nil {
		return err
	}

	length := int(length64)

	if cap(r.readBuf) < length {
		r.readBuf = make([]byte, length*12/10)
	}

	r.readBuf = r.readBuf[:length]
	if _, err := io.ReadFull(r.reader, r.readBuf); err != nil {
		return err
	}

	r.buf.SetBuf(r.readBuf)

	return r.buf.Unmarshal(v)
}

// NewProtoStreamReader returns new ProtoStreamReader on top of a given buffered reader.
// The provided header must match the beginning of a stream.
func NewProtoStreamReader(r *bufio.Reader, header []byte) *ProtoStreamReader {
	return &ProtoStreamReader{
		reader: r,
		header: header,
		buf:    proto.NewBuffer(nil),
	}
}
