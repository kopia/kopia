package encryption

// nullEncryptor implements non-encrypted format.
type nullEncryptor struct {
}

var zeroBytes = []byte{}

func (fi nullEncryptor) Encrypt(output, plainText, contentID []byte) ([]byte, error) {
	if len(plainText) == 0 {
		return zeroBytes, nil
	}

	return append(output, plainText...), nil
}

func (fi nullEncryptor) Decrypt(output, cipherText, contentID []byte) ([]byte, error) {
	if len(cipherText) == 0 {
		return zeroBytes, nil
	}

	return append(output, cipherText...), nil
}

func (fi nullEncryptor) IsAuthenticated() bool {
	return false
}

func (fi nullEncryptor) IsDeprecated() bool {
	return false
}

func init() {
	Register(NoneAlgorithm, "No encryption", false, func(p Parameters) (Encryptor, error) {
		return nullEncryptor{}, nil
	})
}
