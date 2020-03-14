package content

import "testing"

func TestRoundTrip(t *testing.T) {
	cases := []ID{
		"",
		"x",
		"aa",
		"xaa",
		"xaaa",
		"a1x",
	}

	for _, tc := range cases {
		b := contentIDToBytes(nil, tc)

		if got := bytesToContentID(b); got != tc {
			t.Errorf("%q did not round trip, got %q, wanted %q", tc, got, tc)
		}
	}

	if got, want := bytesToContentID(nil), ID(""); got != want {
		t.Errorf("unexpected content id %v, want %v", got, want)
	}
}
