package block

import (
	"reflect"
	"strings"
	"testing"
)

func TestContentID(t *testing.T) {
	cases := []struct {
		contentID ContentID
		want      []byte
	}{
		{"abcdef", []byte{0x00, 0xab, 0xcd, 0xef}},
		{"zabcdef", []byte{0x7a, 0xab, 0xcd, 0xef}},
		{"iabcdef", []byte{0x69, 0xab, 0xcd, 0xef}},
	}

	for _, tc := range cases {
		got, err := packContentID(tc.contentID)
		if err != nil {
			t.Errorf("unable to pack %q: %v", tc.contentID, err)
			continue
		}

		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("invalid packed content for %q: %x wanted %x", tc.contentID, got, tc.want)
		}

		rtt, err := unpackContentID(got)
		if err != nil {
			t.Errorf("unable to round-trip %q: %v", tc.contentID, err)
			continue
		}

		if rtt != tc.contentID {
			t.Errorf("failed to round trip: %q, got %q", tc.contentID, rtt)
		}

	}
}

func TestInvalidContentID(t *testing.T) {
	cases := []struct {
		contentID ContentID
		err       string
	}{
		{"", "invalid content ID"},
		{"a", "invalid content ID"},
		{"aabcdef", "odd length hex string"},
	}

	for _, tc := range cases {
		_, err := packContentID(tc.contentID)
		if err == nil {
			t.Errorf("unexpected success when packing %q, wanted %v", tc.contentID, tc.err)
			continue
		}
		if !strings.Contains(err.Error(), tc.err) {
			t.Errorf("invalid error when packing %q: %v, wanted %q", tc.contentID, err, tc.err)
		}
	}
}
