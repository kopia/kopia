package crypto_test

import (
	"fmt"
	"testing"

	"github.com/kopia/kopia/internal/crypto"
)

var (
	TestMasterKey = []byte("ABCDEFGHIJKLMNOP")
	TestSalt      = []byte("0123456789012345")
	TestPurpose   = []byte("the-test-purpose")
)

func TestDeriveKeyFromMasterKey(t *testing.T) {
	t.Run("ReturnsKey", func(t *testing.T) {
		key := crypto.DeriveKeyFromMasterKey(TestMasterKey, TestSalt, TestPurpose, 32)

		expected := "828769ee8969bc37f11dbaa32838f8db6c19daa6e3ae5f5eed2da2d94d8faddb"
		got := fmt.Sprintf("%02x", key)
		if got != expected {
			t.Errorf("incorrect key\nexpected: %s\n     got: %s", expected, got)
		}
	})

	t.Run("PanicsOnNilMasterKey", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic")
			}
		}()

		crypto.DeriveKeyFromMasterKey(nil, TestSalt, TestPurpose, 32)
	})

	t.Run("PanicsOnEmptyMasterKey", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic")
			}
		}()

		crypto.DeriveKeyFromMasterKey([]byte{}, TestSalt, TestPurpose, 32)
	})
}
