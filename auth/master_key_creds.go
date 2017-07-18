package auth

import "fmt"

type masterKeyCredentials struct {
	key []byte
}

func (mkc *masterKeyCredentials) GetMasterKey(f Options) ([]byte, error) {
	return mkc.key, nil
}

// MasterKey returns master key-based Credentials with the specified key.
func MasterKey(key []byte) (Credentials, error) {
	if len(key) < MinMasterKeyLength {
		return nil, fmt.Errorf("master key too short")
	}

	return &masterKeyCredentials{key}, nil
}
