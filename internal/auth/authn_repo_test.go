package auth_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/crypto"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
)

func TestRepositoryAuthenticator(t *testing.T) {
	a := auth.AuthenticateRepositoryUsers()
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{},
		func(ctx context.Context, w repo.RepositoryWriter) error {
			p := &user.Profile{
				Username: "user1@host1",
			}

			p.SetPassword("password1", crypto.DefaultKeyDerivationAlgorithm)

			return user.SetUserProfile(ctx, w, p)
		}))

	verifyRepoAuthenticator(ctx, t, a, env.Repository, "user1@host1", "password1", true)
	verifyRepoAuthenticator(ctx, t, a, env.Repository, "user1@host1", "password2", false)
	verifyRepoAuthenticator(ctx, t, a, env.Repository, "user1@host1", "password11", false)
	verifyRepoAuthenticator(ctx, t, a, env.Repository, "user1@host1a", "password1", false)
	verifyRepoAuthenticator(ctx, t, a, env.Repository, "user1@host1a", "password1a", false)
}

func verifyRepoAuthenticator(ctx context.Context, t *testing.T, a auth.Authenticator, r repo.Repository, username, password string, want bool) {
	t.Helper()

	if got := a.IsValid(ctx, r, username, password); got != want {
		t.Errorf("invalid authenticator result for %v/%v: %v, want %v", username, password, got, want)
	}
}
