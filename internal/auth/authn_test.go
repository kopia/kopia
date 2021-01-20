package auth_test

import (
	"context"
	"testing"

	"github.com/kopia/kopia/internal/auth"
)

func TestAuthentication(t *testing.T) {
	a := auth.AuthenticateSingleUser("user1", "password1")
	verifyAuthenticator(t, a, "user1", "password1", true)
	verifyAuthenticator(t, a, "user1", "password2", false)
	verifyAuthenticator(t, a, "user1", "password11", false)
	verifyAuthenticator(t, a, "user1a", "password1", false)
	verifyAuthenticator(t, a, "user1a", "password1a", false)
}

func verifyAuthenticator(t *testing.T, a auth.Authenticator, username, password string, want bool) {
	t.Helper()

	if got := a(context.Background(), nil, username, password); got != want {
		t.Errorf("invalid authenticator result for %v/%v: %v, want %v", username, password, got, want)
	}
}
