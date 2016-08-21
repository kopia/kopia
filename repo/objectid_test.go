package repo

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestParseMalformedObjectID(t *testing.T) {
	cases := []string{
		"",
		"B!$@#$!@#$",
		"X",
		"D.",
		"D.x",
		"D.af",
		"Dx.ag",
		"Dab.",
		"Dab.a",
		"Dab.abc",
		"Dab.011",
		"L.",
		"L.x",
		"L.af",
		"Lx.ag",
		"Lab.",
		"L1",
		"L1,",
		"L-1,X",
		"Xsomething",
	}

	for _, c := range cases {
		v, err := ParseObjectID(c)
		if !reflect.DeepEqual(v, NullObjectID) || err == nil || !strings.HasPrefix(err.Error(), "malformed object id") {
			t.Errorf("unexpected result for %v: v: %v err: %v", c, v, err)
		}
	}
}

func TestParseObjectIDEncryptionInfo(t *testing.T) {
	cases := []struct {
		objectID string
		expected []byte
	}{
		{"B", nil},
		{"BAQIDBA", nil},
		{"Dabcdef", nil},
		{"I1,abcdef", nil},
		{"I2,abcdef", nil},
		{"Dabcdef.00112233", []byte{0x00, 0x11, 0x22, 0x33}},
		{"I1,abcdef.0011223344", []byte{0x00, 0x11, 0x22, 0x33, 0x44}},
		{"I2,abcdef.0011223344", []byte{0x00, 0x11, 0x22, 0x33, 0x44}},
	}

	for _, c := range cases {
		objectID, err := ParseObjectID(c.objectID)
		if err != nil {
			t.Errorf("cannot parse object ID: %v", err)
			continue
		}

		actual := objectID.Encryption
		if !bytes.Equal(actual, c.expected) {
			t.Errorf("invalid encryption info for %v: %x, expected: %x", c.objectID, actual, c.expected)
		}
	}
}
