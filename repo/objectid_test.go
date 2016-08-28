package repo

import (
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
		expected string
	}{
		{"B", ""},
		{"BAQIDBA", ""},
		{"Dabcdef", ""},
		{"I1,abcdef", ""},
		{"I2,abcdef", ""},
		{"Dabcdef.00112233", "00112233"},
		{"I1,abcdef.0011223344", "0011223344"},
		{"I2,abcdef.0011223344", "0011223344"},
		{"S1,2,Dabcdef.0011223344", ""},
		{"S1,2,S3,4,Dabcdef.0011223344", ""},
	}

	for _, c := range cases {
		objectID, err := ParseObjectID(c.objectID)
		if err != nil {
			t.Errorf("cannot parse object ID: %v", err)
			continue
		}

		actual := objectID.EncryptionKey
		if actual != c.expected {
			t.Errorf("invalid encryption key for %v: %v, expected: %v", c.objectID, actual, c.expected)
		}

		uiString := objectID.UIString()
		if uiString != c.objectID {
			t.Errorf("invalid object ID string: %v: expected: %v", uiString, c.objectID)
		}
	}
}
