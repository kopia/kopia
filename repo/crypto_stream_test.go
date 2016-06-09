package repo

import (
	"bytes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
	"io/ioutil"

	"github.com/kopia/kopia/blob"

	"testing"
)

type fakeStreamCipher struct {
	xor byte
}

func (nsc *fakeStreamCipher) XORKeyStream(dst, src []byte) {
	for i := 0; i < len(src); i++ {
		dst[i] = src[i] ^ nsc.xor
	}
}

func TestCryptoStream(t *testing.T) {
	for _, s := range []struct {
		cipher   cipher.Stream
		data     string
		expected string
	}{
		{
			cipher:   &fakeStreamCipher{0},
			data:     "cafeb1bab3c0",
			expected: "cafeb1bab3c0",
		},
		{
			cipher:   &fakeStreamCipher{1},
			data:     "cafeb1bab3c0",
			expected: "cbffb0bbb2c1",
		},
	} {
		data, err := hex.DecodeString(s.data)
		if err != nil {
			t.Errorf("error decoding data: %v", err)
			continue
		}
		enc := newEncryptingReader(
			blob.NewReader(bytes.NewBuffer(data)),
			s.cipher)
		v, err := ioutil.ReadAll(enc)
		actual := fmt.Sprintf("%x", v)
		if err != nil {
			t.Errorf("expected %v got error: %v", s.expected, err)
		}

		if actual != s.expected {
			t.Errorf("expected %v got: %v", s.expected, actual)
		}
	}
}
