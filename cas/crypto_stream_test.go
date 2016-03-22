package cas

import (
	"bytes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"hash"
	"io/ioutil"

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
		hash     func() hash.Hash
		header   string
		data     string
		expected string
	}{
		{
			cipher:   &fakeStreamCipher{},
			hash:     sha1.New,
			header:   "",
			data:     "",
			expected: "da39a3ee5e6b4b0d3255bfef95601890afd80709", // SHA1 of empty string
		},
		{
			cipher:   &fakeStreamCipher{},
			hash:     md5.New,
			header:   "",
			data:     "",
			expected: "d41d8cd98f00b204e9800998ecf8427e", // MD5 of empty string
		},
		{
			cipher:   &fakeStreamCipher{},
			hash:     sha1.New,
			header:   "0000",
			data:     "",
			expected: "00001489f923c4dca729178b3e3233458550d8dddf29",
		},
		{
			cipher:   &fakeStreamCipher{},
			hash:     sha1.New,
			header:   "1234",
			data:     "",
			expected: "1234ffa76d854a2969e7b9d83868d455512fce0fd74d",
		},
		{
			cipher:   &fakeStreamCipher{},
			hash:     sha1.New,
			header:   "deadbeef",
			data:     "cafeb1bab3c0",
			expected: "deadbeefcafeb1bab3c00b01e595963c80cee1e04a6c1079dc2e186a553f",
		},
		{
			cipher:   &fakeStreamCipher{0},
			hash:     func() hash.Hash { return hmac.New(sha1.New, []byte{1, 2, 3}) },
			header:   "deadbeef",
			data:     "cafeb1bab3c0",
			expected: "deadbeefcafeb1bab3c0a5c88d0104f5fafc8ee629002104199b523665e5",
		},
	} {
		header, err := hex.DecodeString(s.header)
		if err != nil {
			t.Errorf("error decoding IV: %v", err)
			continue
		}
		data, err := hex.DecodeString(s.data)
		if err != nil {
			t.Errorf("error decoding data: %v", err)
			continue
		}
		enc := newEncryptingReader(
			ioutil.NopCloser(bytes.NewReader(data)),
			header,
			s.cipher,
			s.hash())
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
