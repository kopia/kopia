package user_test

import (
	"testing"

	"github.com/kopia/kopia/internal/crypto"
	"github.com/kopia/kopia/internal/user"
)

func TestUserProfile(t *testing.T) {
	p := &user.Profile{}

	if p.IsValidPassword("bar", crypto.DefaultKeyDerivationAlgorithm) {
		t.Fatalf("password unexpectedly valid!")
	}

	p.SetPassword("foo", crypto.DefaultKeyDerivationAlgorithm)

	if !p.IsValidPassword("foo", crypto.DefaultKeyDerivationAlgorithm) {
		t.Fatalf("password not valid!")
	}

	if p.IsValidPassword("bar", crypto.DefaultKeyDerivationAlgorithm) {
		t.Fatalf("password unexpectedly valid!")
	}
}

func TestBadKeyDerivationAlgorithmPanic(t *testing.T) {
	defer func() { _ = recover() }()
	func() {
		p := &user.Profile{}
		p.SetPassword("foo", crypto.DefaultKeyDerivationAlgorithm)
		p.IsValidPassword("foo", "bad algorithm")
	}()
	t.Errorf("should have panicked")
}

func TestNilUserProfile(t *testing.T) {
	var p *user.Profile

	if p.IsValidPassword("bar", crypto.DefaultKeyDerivationAlgorithm) {
		t.Fatalf("password unexpectedly valid!")
	}
}

func TestInvalidPasswordHash(t *testing.T) {
	cases := [][]byte{
		[]byte("**invalid*base64*"),
		[]byte(""),
	}

	for _, tc := range cases {
		p := &user.Profile{PasswordHash: tc}
		if p.IsValidPassword("some-password", crypto.DefaultKeyDerivationAlgorithm) {
			t.Fatalf("password unexpectedly valid for %v", tc)
		}
	}
}
