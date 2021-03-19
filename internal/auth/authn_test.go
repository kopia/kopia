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

func TestCombineAuthenticators_Empty(t *testing.T) {
	a := auth.CombineAuthenticators()
	if a != nil {
		t.Fatal("combined authenticator expected to return nil for zero-length input")
	}
}

func TestCombineAuthenticators(t *testing.T) {
	a1 := auth.AuthenticateSingleUser("user1", "password1")
	a2 := auth.AuthenticateSingleUser("user2", "password2")
	a3 := auth.AuthenticateSingleUser("user3", "password3")

	a := auth.CombineAuthenticators(a1, a2, a3)
	verifyAuthenticator(t, a, "user1", "password1", true)
	verifyAuthenticator(t, a, "user2", "password2", true)
	verifyAuthenticator(t, a, "user3", "password3", true)
	verifyAuthenticator(t, a, "user1", "password2", false)
}

func verifyAuthenticator(t *testing.T, a auth.Authenticator, username, password string, want bool) {
	t.Helper()

	if got := a.IsValid(context.Background(), nil, username, password); got != want {
		t.Errorf("invalid authenticator result for %v/%v: %v, want %v", username, password, got, want)
	}
}
