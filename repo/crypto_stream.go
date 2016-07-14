package repo

import (
	"crypto/cipher"
	"fmt"
	"io"

	"github.com/kopia/kopia/storage"
)

// encryptingReader wraps an io.Reader and returns data encrypted using a stream cipher
type encryptingReader struct {
	source io.Reader
	closer io.Closer
	cipher cipher.Stream
	length int
}

func (er *encryptingReader) Len() int {
	return er.length
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
	if er.closer != nil {
		return er.closer.Close()
	}

	return nil
}

func (er *encryptingReader) String() string {
	return fmt.Sprintf("encryptingReader(%v)", er.source)
}

func newEncryptingReader(source storage.ReaderWithLength, c cipher.Stream) storage.ReaderWithLength {
	return &encryptingReader{
		source: source,
		closer: source,
		length: source.Len(),
		cipher: c,
	}
}
