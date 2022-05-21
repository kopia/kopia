package index

import "testing"

func TestRoundTrip(t *testing.T) {
	cases := []ID{
		mustParseID(t, ""),
		mustParseID(t, "aa"),
		mustParseID(t, "xaa"),
	}

	for _, tc := range cases {
		b := contentIDToBytes(nil, tc)

		if got := bytesToContentID(b); got != tc {
			t.Errorf("%q did not round trip, got %q, wanted %q", tc, got, tc)
		}
	}

	if got, want := bytesToContentID(nil), EmptyID; got != want {
		t.Errorf("unexpected content id %v, want %v", got, want)
	}
}
