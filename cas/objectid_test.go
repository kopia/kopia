package cas

import (
	"strings"
	"testing"
)

func TestParseMalformedObjectID(t *testing.T) {
	cases := []string{
		"",
		"B!$@#$!@#$",
		"X",
		"D:",
		"D:x",
		"D:af",
		"Dx:ag",
		"Dab:",
		"Dab:a",
		"Dab:abc",
		"Dab:00",
		"Dab:011",
		"L:",
		"L:x",
		"L:af",
		"Lx:ag",
		"Lab:",
		"Xsomething",
	}

	for _, c := range cases {
		v, err := ParseObjectID(c)
		if v != "" || err == nil || !strings.HasPrefix(err.Error(), "malformed chunk id") {
			t.Errorf("unexpected result for %v: v: %v err: %v", c, v, err)
		}
	}
}

func TestParseObjectIDEncryptionInfo(t *testing.T) {
	cases := []struct {
		objectID string
		expected ObjectEncryptionInfo
	}{
		{"B", NoEncryption},
		{"BAQIDBA", NoEncryption},
		{"T", NoEncryption},
		{"T:foo", NoEncryption},
		{"Tfoo\nbar", NoEncryption},
		{"Tfoo\nbar:baz", NoEncryption},
		{"Dabcdef", NoEncryption},
		{"Labcdef", NoEncryption},
		{
			"Dabcdef:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
			ObjectEncryptionInfo("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"),
		},
		{
			"Labcdef:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
			ObjectEncryptionInfo("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"),
		},
	}

	for _, c := range cases {
		objectID, err := ParseObjectID(c.objectID)
		if err != nil {
			t.Errorf("cannot parse object ID: %v", err)
			continue
		}

		actual := objectID.EncryptionInfo()
		if actual != c.expected {
			t.Errorf("invalid encryption info for %v: %v, expected: %v", c.objectID, actual, c.expected)
		}
	}
}
