package repo

import (
	"crypto/cipher"
	"io"

	"github.com/kopia/kopia/blob"
)

// encryptingReader wraps an io.Reader and returns data encrypted using a stream cipher
type encryptingReader struct {
	source blob.BlockReader
	closer io.Closer

	cipher cipher.Stream
}

func (er *encryptingReader) Len() int {
	return er.source.Len()
}

func (er *encryptingReader) Read(b []byte) (int, error) {
	read := 0
	for len(b) > 0 {
		switch {
		case er.source != nil:
			n, err := er.source.Read(b)
			got := b[0:n]
			er.cipher.XORKeyStream(got, got)
			read += n
			b = b[n:]
			if err == io.EOF {
				er.source = nil
			} else if err != nil {
				return read, err
			}

		default:
			return read, io.EOF
		}
	}
	return read, nil
}

func (er *encryptingReader) Close() error {
	if er.source != nil {
		return er.source.Close()
	}

	return nil
}

func newEncryptingReader(source blob.BlockReader, c cipher.Stream) blob.BlockReader {
	return &encryptingReader{
		source: source,
		cipher: c,
	}
}
