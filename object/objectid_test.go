package object

import (
	"testing"
)

func TestParseObjectID(t *testing.T) {
	cases := []struct {
		text    string
		isValid bool
	}{
		{"Df0f0", true},
		{"IDf0f0", true},
		{"IDf0f0", true},
		{"IIDf0f0", true},
		{"Dxf0f0", true},
		{"IDxf0f0", true},
		{"IDxf0f0", true},
		{"IIDxf0f0", true},
		{"Dxf0f", false},
		{"IDxf0f", false},
		{"Da", false},
		{"Daf0f0", false},
		{"", false},
		{"B!$@#$!@#$", false},
		{"X", false},
		{"I.", false},
		{"I.x", false},
		{"I.af", false},
		{"Ix.ag", false},
		{"Iab.", false},
		{"I1", false},
		{"I1,", false},
		{"I-1,X", false},
		{"Xsomething", false},
	}

	for _, tc := range cases {
		_, err := ParseID(tc.text)
		if err != nil && tc.isValid {
			t.Errorf("error parsing %q: %v", tc.text, err)
		} else if err == nil && !tc.isValid {
			t.Errorf("unexpected success parsing %v", tc.text)
		}
	}
}
