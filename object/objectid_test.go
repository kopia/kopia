package object

import (
	"reflect"
	"strings"
	"testing"
)

type rawObjectID ObjectID

func TestParseObjectID(t *testing.T) {
	cases := []struct {
		Text     string
		ObjectID ObjectID
	}{
		{"Dfoo", ObjectID{StorageBlock: "foo"}},
		{"IDfoo", ObjectID{Indirect: &ObjectID{StorageBlock: "foo"}}},
		{"I1,foo", ObjectID{Indirect: &ObjectID{StorageBlock: "foo"}}},
		{"I2,foo", ObjectID{Indirect: &ObjectID{Indirect: &ObjectID{StorageBlock: "foo"}}}},
		{"IDfoo", ObjectID{Indirect: &ObjectID{StorageBlock: "foo"}}},
		{"IIDfoo", ObjectID{Indirect: &ObjectID{Indirect: &ObjectID{StorageBlock: "foo"}}}},
		{"Pfoo@bar", ObjectID{StorageBlock: "foo"}}, // legacy
		{"S1,2,Dfoo", ObjectID{Section: &ObjectIDSection{Start: 1, Length: 2, Base: ObjectID{StorageBlock: "foo"}}}},
	}

	for _, tc := range cases {
		oid, err := ParseObjectID(tc.Text)
		if err != nil {
			t.Errorf("error parsing %q: %v", tc.Text, err)
		}

		if !reflect.DeepEqual(oid, tc.ObjectID) {
			t.Errorf("invalid result for %q: %+v, wanted %+v", tc.Text, rawObjectID(oid), rawObjectID(tc.ObjectID))
		}

		oid2, err := ParseObjectID(oid.String())
		if err != nil {
			t.Errorf("parse error %q: %v", oid.String(), err)
		} else if !reflect.DeepEqual(oid, oid2) {
			t.Errorf("does not round-trip: %v (%+v), got %+v", oid.String(), rawObjectID(oid), rawObjectID(oid2))
		}
	}
}

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
