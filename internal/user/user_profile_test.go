package user_test

import (
	"testing"

	"github.com/kopia/kopia/internal/user"
)

func TestUserProfile(t *testing.T) {
	p := &user.Profile{}

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
