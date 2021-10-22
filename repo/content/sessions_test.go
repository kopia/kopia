package content

import (
	"testing"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/blob"
)

func TestGenerateSessionID(t *testing.T) {
	n := clock.WallClockTime()

	s1, err := generateSessionID(n)
	if err != nil {
		t.Fatal(err)
	}

	s2, err := generateSessionID(n)
	if err != nil {
		t.Fatal(err)
	}

	s3, err := generateSessionID(n)
	if err != nil {
		t.Fatal(err)
	}

	m := map[SessionID]bool{
		s1: true,
		s2: true,
		s3: true,
	}

	if len(m) != 3 {
		t.Fatalf("session IDs were not unique: %v", m)
	}
}

func TestSessionIDFromBlobID(t *testing.T) {
	cases := []struct {
		blobID    blob.ID
		sessionID SessionID
	}{
		{"pdeadbeef", ""},
		{"pdeadbeef-", ""},
		{"pdeadbeef-whatever", ""},
		{"pdeadbeef-s01", "s01"},
		{"pdeadbeef-s01", "s01"},
		{"sdeadbeef-s01", "s01"},
	}

	for _, tc := range cases {
		if got, want := SessionIDFromBlobID(tc.blobID), tc.sessionID; got != want {
			t.Errorf("invalid result for %v: %v, want %v", tc.blobID, got, want)
		}
	}
}
