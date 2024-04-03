package user_test

import (
	"testing"

	"github.com/kopia/kopia/internal/crypto"
	"github.com/kopia/kopia/internal/user"
)

func TestLegacyUserProfile(t *testing.T) {
	p := &user.Profile{
		PasswordHashVersion: 1, // hashVersion1
	}

	if p.IsValidPassword("bar") {
		t.Fatalf("password unexpectedly valid!")
	}

	p.SetPassword("foo")

	if !p.IsValidPassword("foo") {
		t.Fatalf("password not valid!")
	}

	if p.IsValidPassword("bar") {
		t.Fatalf("password unexpectedly valid!")
	}
}

func TestUserProfile(t *testing.T) {
	p := &user.Profile{
		KeyDerivationAlgorithm: crypto.ScryptAlgorithm,
	}

	if p.IsValidPassword("bar") {
		t.Fatalf("password unexpectedly valid!")
	}

	p.SetPassword("foo")

	if !p.IsValidPassword("foo") {
		t.Fatalf("password not valid!")
	}

	if p.IsValidPassword("bar") {
		t.Fatalf("password unexpectedly valid!")
	}

	// Different key derivation algorithm besides the original should fail
	p.KeyDerivationAlgorithm = crypto.Pbkdf2Algorithm
	if p.IsValidPassword("foo") {
		t.Fatalf("password unexpectedly valid!")
	}

}

func TestBadKeyDerivationAlgorithmPanic(t *testing.T) {
	defer func() { _ = recover() }()
	func() {
		// mock a valid password
		p := &user.Profile{
			KeyDerivationAlgorithm: crypto.ScryptAlgorithm,
		}
		p.SetPassword("foo")
		// Assume the key derivation algorithm is bad. This will cause
		// a panic when validating
		p.KeyDerivationAlgorithm = "some bad algorithm"
		p.IsValidPassword("foo")
	}()
	t.Errorf("should have panicked")
}

func TestNilUserProfile(t *testing.T) {
	var p *user.Profile

	if p.IsValidPassword("bar") {
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
		if p.IsValidPassword("some-password") {
			t.Fatalf("password unexpectedly valid for %v", tc)
		}
	}
}
