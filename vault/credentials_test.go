package vault

import (
	"bytes"
	"encoding/hex"

	"testing"
)

func TestCredentials(t *testing.T) {
	cases := []struct {
		username    string
		password    string
		expectedKey string
	}{
		{"foo", "bar", "60d6051cfbff0f53344ff64cd9770c65747ced5c541748b7f992cf575bffa2ad"},
		{"user", "bar", "fff2b04b391c1a31a41dab88843311ce7f93393ec97fb8a1be3697c5a88b85ca"},
	}

	for i, c := range cases {
		creds := Password(c.username, c.password)
		if u := creds.Username(); u != c.username {
			t.Errorf("invalid username #%v: %v expected %v", i, u, c.username)
		}

		expectedKeyBytes, _ := hex.DecodeString(c.expectedKey)
		if v := creds.PrivateKey(); !bytes.Equal(expectedKeyBytes, v.Bytes()) {
			t.Errorf("invalid key #%v: expected %x, got: %x", i, expectedKeyBytes, v.Bytes())
		}
	}
}
