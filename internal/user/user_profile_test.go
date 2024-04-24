package user_test

import (
	"testing"

	"github.com/kopia/kopia/internal/user"
)

func TestUserProfile(t *testing.T) {
	p := &user.Profile{
		PasswordHashVersion: user.ScryptHashVersion,
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
	p.PasswordHashVersion = user.Pbkdf2HashVersion
	if p.IsValidPassword("foo") {
		t.Fatalf("password unexpectedly valid!")
	}
}

func TestBadPasswordHashVersion(t *testing.T) {
	// mock a valid password
	p := &user.Profile{
		PasswordHashVersion: user.ScryptHashVersion,
	}
	p.SetPassword("foo")
	// Assume the key derivation algorithm is bad. This will cause
	// a panic when validating
	p.PasswordHashVersion = 0
	if p.IsValidPassword("foo") {
		t.Fatalf("password unexpectedly valid!")
	}
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
		p := &user.Profile{
			PasswordHash:        tc,
			PasswordHashVersion: 1,
		}
		if p.IsValidPassword("some-password") {
			t.Fatalf("password unexpectedly valid for %v", tc)
		}
	}
}
