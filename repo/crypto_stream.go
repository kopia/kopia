package repo

import (
	"bytes"
	"crypto/cipher"
	"hash"
	"io"
)

// encryptingReader wraps an io.Reader and returns data encrypted using a stream cipher
type encryptingReader struct {
	source   io.Reader
	header   io.Reader
	checksum io.Reader
	closer   io.Closer

	cipher cipher.Stream
	hash   hash.Hash
}

func (er *encryptingReader) Read(b []byte) (int, error) {
	read := 0
	for len(b) > 0 {
		switch {
		case er.header != nil:
			n, err := er.header.Read(b)
			er.addToChecksum(b[0:n])
			read += n
			b = b[n:]
			if err == io.EOF {
				er.header = nil
			} else if err != nil {
				return read, err
			}

		case er.source != nil:
			n, err := er.source.Read(b)
			got := b[0:n]
			er.cipher.XORKeyStream(got, got)
			er.addToChecksum(got)
			read += n
			b = b[n:]
			if err == io.EOF {
				er.source = nil
				if er.hash != nil {
					er.checksum = bytes.NewReader(er.hash.Sum(nil))
				}
			} else if err != nil {
				return read, err
			}

		case er.checksum != nil:
			n, err := er.checksum.Read(b)
			b = b[n:]
			read += n
			if err == io.EOF {
				er.checksum = nil
			}
			if err != nil {
				return read, err
			}

		default:
			return read, io.EOF
		}
	}
	return read, nil
}

func (er *encryptingReader) addToChecksum(b []byte) {
	if er.hash != nil {
		n, err := er.hash.Write(b)
		if err != nil || n != len(b) {
			panic("unexpected hashing error")
		}
	}
}

func (er *encryptingReader) Close() error {
	if er.closer != nil {
		return er.closer.Close()
	}

	return nil
}

func newEncryptingReader(source io.ReadCloser, header []byte, c cipher.Stream, hash hash.Hash) io.ReadCloser {
	return &encryptingReader{
		source: source,
		header: bytes.NewReader(header),
		cipher: c,
		hash:   hash,
		closer: source,
	}
}
