package repo

import "testing"

func TestFormat(t *testing.T) {
	data := make([]byte, 100)
	secret := []byte("secret")

	for k, v := range SupportedFormats {
		t.Logf("testing %v", k)
		blk, key := v.HashBuffer(data, secret)
		if key != nil {
			cipher, err := v.CreateCipher(key)
			if err != nil || cipher == nil {
				t.Errorf("invalid response from CreateCipher: %v %v", cipher, err)
			}
		} else {
			cipher, err := v.CreateCipher(key)
			if err == nil || cipher != nil {
				t.Errorf("expected failure, but got response from CreateCipher: %v %v", cipher, err)
			}
		}

		if len(blk)%16 != 0 {
			t.Errorf("block ID for %v not a multiple of 16: %v", k, len(blk))
		}
	}
}
