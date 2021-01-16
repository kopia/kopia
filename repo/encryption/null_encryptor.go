package encryption

// nullEncryptor implements non-encrypted format.
type nullEncryptor struct{}

func (fi nullEncryptor) Encrypt(output, plainText, contentID []byte) ([]byte, error) {
	return append(output, plainText...), nil
}

func (fi nullEncryptor) Decrypt(output, cipherText, contentID []byte) ([]byte, error) {
	return append(output, cipherText...), nil
}

func (fi nullEncryptor) IsAuthenticated() bool {
	return false
}

func (fi nullEncryptor) IsDeprecated() bool {
	return false
}

func (fi nullEncryptor) MaxOverhead() int {
	return 0
}

func init() {
	Register(DeprecatedNoneAlgorithm, "No encryption", true, func(p Parameters) (Encryptor, error) {
		return nullEncryptor{}, nil
	})
}
