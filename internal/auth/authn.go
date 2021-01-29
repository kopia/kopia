// Package auth provides authentication and authorization constructs.
package auth

import (
	"context"
	"crypto/subtle"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.GetContextLoggerFunc("auth")

// Authenticator verifies that the provided username/password is valid.
type Authenticator func(ctx context.Context, rep repo.Repository, username, password string) bool

// AuthenticateSingleUser returns an Authenticator that only allows one username/password combination.
func AuthenticateSingleUser(expectedUsername, expectedPassword string) Authenticator {
	expectedUsernameBytes := []byte(expectedUsername)
	expectedPasswordBytes := []byte(expectedPassword)

	return func(ctx context.Context, rep repo.Repository, username, password string) bool {
		return subtle.ConstantTimeCompare([]byte(username), expectedUsernameBytes)*
			subtle.ConstantTimeCompare([]byte(password), expectedPasswordBytes) == 1
	}
}
