package content

import (
	"strings"
	"testing"
)

func TestParseMalformedObjectID(t *testing.T) {
	cases := []string{
		"",
		"B!$@#$!@#$",
		"X",
		"C:",
		"C:x",
		"C:af",
		"Cx:ag",
		"Cab:",
		"Cab:a",
		"Cab:abc",
		"Cab:00",
		"Cab:011",
		"L:",
		"L:x",
		"L:af",
		"Lx:ag",
		"Lab:",
		"Xsomething",
	}

	for _, c := range cases {
		v, err := ParseObjectID(c)
		if v != NullObjectID || err == nil || !strings.HasPrefix(err.Error(), "malformed chunk id") {
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
		{"BAQIDBA==", NoEncryption},
		{"T", NoEncryption},
		{"T:foo", NoEncryption},
		{"Tfoo\nbar", NoEncryption},
		{"Tfoo\nbar:baz", NoEncryption},
		{"Cabcdef", NoEncryption},
		{"Labcdef", NoEncryption},
		{
			"Cabcdef:0100112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
			ObjectEncryptionInfo("0100112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"),
		},
		{
			"Labcdef:0100112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
			ObjectEncryptionInfo("0100112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"),
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
