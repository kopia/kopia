//go:build testing
// +build testing

package crypto

import (
	"crypto/sha256"
)

const TestingOnlyInsecurePBKeyDerivationAlgorithm = "testing-only-insecure"

func init() {
	registerPBKeyDeriver(TestingOnlyInsecurePBKeyDerivationAlgorithm, &insecureKeyDeriver{})
}

type insecureKeyDeriver struct{}

func (s *insecureKeyDeriver) deriveKeyFromPassword(password string, salt []byte, keySize int) ([]byte, error) {
	h := sha256.New()
	if _, err := h.Write([]byte(password)); err != nil {
		return nil, err
	}

	return h.Sum(nil)[:keySize], nil
}
