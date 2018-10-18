package packindex

import "testing"

func TestRoundTrip(t *testing.T) {
	cases := []string{
		"",
		"x",
		"aa",
		"xaa",
		"xaaa",
		"a1x",
	}

	for _, tc := range cases {
		b := contentIDToBytes(tc)
		got := bytesToContentID(b)
		if got != tc {
			t.Errorf("%q did not round trip, got %q, wanted %q", tc, got, tc)
		}
	}

	if got, want := bytesToContentID(nil), ""; got != want {
		t.Errorf("unexpected content id %v, want %v", got, want)
	}
}
