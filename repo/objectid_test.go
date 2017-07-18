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
