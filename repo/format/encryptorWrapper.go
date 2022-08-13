package format

import (
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/encryption"
)

type encryptorWrapper struct {
	impl encryption.Encryptor
	next encryption.Encryptor
}

func (p *encryptorWrapper) Encrypt(plainText gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	var tmp gather.WriteBuffer
	defer tmp.Close()

	if err := p.impl.Encrypt(plainText, contentID, &tmp); err != nil {
		//nolint:wrapcheck
		return err
	}

	//nolint:wrapcheck
	return p.next.Encrypt(tmp.Bytes(), contentID, output)
}

func (p *encryptorWrapper) Decrypt(cipherText gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	var tmp gather.WriteBuffer
	defer tmp.Close()

	if err := p.next.Decrypt(cipherText, contentID, &tmp); err != nil {
		//nolint:wrapcheck
		return err
	}

	//nolint:wrapcheck
	return p.impl.Decrypt(tmp.Bytes(), contentID, output)
}

func (p *encryptorWrapper) Overhead() int {
	panic("Should not be called")
}
