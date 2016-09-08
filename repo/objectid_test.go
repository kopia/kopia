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
		"I.",
		"I.x",
		"I.af",
		"Ix.ag",
		"Iab.",
		"I1",
		"I1,",
		"I-1,X",
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
		{"S1,2,Dabcdef.0011223344", nil},
		{"S1,2,S3,4,Dabcdef.0011223344", nil},
	}

	for _, c := range cases {
		objectID, err := ParseObjectID(c.objectID)
		if err != nil {
			t.Errorf("cannot parse object ID: %v", err)
			continue
		}

		actual := objectID.EncryptionKey
		if !bytes.Equal(actual, c.expected) {
			t.Errorf("invalid encryption key for %v: %x, expected: %x", c.objectID, actual, c.expected)
		}

		uiString := objectID.String()
		if uiString != c.objectID {
			t.Errorf("invalid object ID string: %v: expected: %v", uiString, c.objectID)
		}
	}
}
