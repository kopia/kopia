package encryption

// nullEncryptor implements non-encrypted format.
type nullEncryptor struct {
}

func (fi nullEncryptor) Encrypt(plainText, contentID []byte) ([]byte, error) {
	return cloneBytes(plainText), nil
}

func (fi nullEncryptor) Decrypt(cipherText, contentID []byte) ([]byte, error) {
	return cloneBytes(cipherText), nil
}

func (fi nullEncryptor) IsAuthenticated() bool {
	return false
}

func init() {
	Register(NoneAlgorithm, "No encryption", false, func(p Parameters) (Encryptor, error) {
		return nullEncryptor{}, nil
	})
}
