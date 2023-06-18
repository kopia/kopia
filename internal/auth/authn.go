// Package auth provides authentication and authorization constructs.
package auth

import (
	"context"
	"crypto/subtle"

	"github.com/pkg/errors"
	"github.com/tg123/go-htpasswd"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("auth")

// Authenticator verifies that the provided username/password is valid.
type Authenticator interface {
	IsValid(ctx context.Context, rep repo.Repository, username, password string) bool
	Refresh(ctx context.Context) error
}

type singleUserAuthenticator struct {
	expectedUsernameBytes []byte
	expectedPasswordBytes []byte
}

func (a *singleUserAuthenticator) IsValid(ctx context.Context, _ repo.Repository, username, password string) bool {
	return subtle.ConstantTimeCompare([]byte(username), a.expectedUsernameBytes)*
		subtle.ConstantTimeCompare([]byte(password), a.expectedPasswordBytes) == 1
}

func (a *singleUserAuthenticator) Refresh(ctx context.Context) error {
	return nil
}

// AuthenticateSingleUser returns an Authenticator that only allows one username/password combination.
func AuthenticateSingleUser(expectedUsername, expectedPassword string) Authenticator {
	return &singleUserAuthenticator{[]byte(expectedUsername), []byte(expectedPassword)}
}

type combinedAuthenticator []Authenticator

func (c combinedAuthenticator) IsValid(ctx context.Context, rep repo.Repository, username, password string) bool {
	for _, a := range c {
		if a.IsValid(ctx, rep, username, password) {
			return true
		}
	}

	return false
}

func (c combinedAuthenticator) Refresh(ctx context.Context) error {
	for _, a := range c {
		if err := a.Refresh(ctx); err != nil {
			return errors.Wrap(err, "error refreshing authenticator")
		}
	}

	return nil
}

// CombineAuthenticators return authenticator that applies the provided authenticators in order
// and returns true if any of them accepts given username/password combination.
func CombineAuthenticators(authenticators ...Authenticator) Authenticator {
	if len(authenticators) == 0 {
		return nil
	}

	return combinedAuthenticator(authenticators)
}

type htpasswdAuthenticator struct {
	f *htpasswd.File
}

func (a htpasswdAuthenticator) IsValid(ctx context.Context, _ repo.Repository, username, password string) bool {
	return a.f.Match(username, password)
}

func (a htpasswdAuthenticator) Refresh(ctx context.Context) error {
	return errors.Wrap(a.f.Reload(nil), "error reloading password file")
}

// AuthenticateHtpasswdFile returns an authenticator that accepts users in the provided htpasswd file.
func AuthenticateHtpasswdFile(f *htpasswd.File) Authenticator {
	return htpasswdAuthenticator{f}
}
