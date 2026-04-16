package auth_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
)

func TestRepositoryAuthenticator(t *testing.T) {
	a := auth.AuthenticateRepositoryUsers()
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{},
		func(ctx context.Context, w repo.RepositoryWriter) error {
			testProfile := user.Profile{
				Username:            "user1@host1",
				PasswordHashVersion: user.ScryptHashVersion,
			}

			err := testProfile.SetPassword("password1")
			if err != nil {
				return err
			}

			err = user.SetUserProfile(ctx, w, &testProfile)
			if err != nil {
				return err
			}

			return nil
		}))

	// valid user, valid password
	verifyRepoAuthenticator(ctx, t, a, env.Repository, "user1@host1", "password1", true)
	// valid user, invalid password
	verifyRepoAuthenticator(ctx, t, a, env.Repository, "user1@host1", "password2", false)
	// valid user, invalid password
	verifyRepoAuthenticator(ctx, t, a, env.Repository, "user1@host1", "password11", false)
	// invalid user, existing password
	verifyRepoAuthenticator(ctx, t, a, env.Repository, "user1@host1a", "password1", false)
	// invalid user, invalid password
	verifyRepoAuthenticator(ctx, t, a, env.Repository, "user1@host1a", "password1a", false)
}

func TestRepositoryAuthenticatorPasswordHashVersion(t *testing.T) {
	for _, tc := range []struct {
		profile  *user.Profile
		password string
	}{
		{
			profile: &user.Profile{
				Username:            "user2@host2",
				PasswordHashVersion: user.ScryptHashVersion,
			},
			password: "password2",
		},
		{
			profile: &user.Profile{
				Username: "user3@host3",
				// PasswordHashVersion is not set
			},
			password: "password3",
		},
		{
			profile: &user.Profile{
				Username:            "user4@host4",
				PasswordHashVersion: user.Pbkdf2HashVersion,
			},
			password: "password4",
		},
	} {
		t.Run(strconv.Itoa(tc.profile.PasswordHashVersion), func(t *testing.T) {
			a := auth.AuthenticateRepositoryUsers()
			ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

			require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{},
				func(ctx context.Context, w repo.RepositoryWriter) error {
					err := tc.profile.SetPassword(tc.password)
					if err != nil {
						return err
					}

					err = user.SetUserProfile(ctx, w, tc.profile)
					if err != nil {
						return err
					}

					return nil
				}))

			verifyRepoAuthenticator(ctx, t, a, env.Repository, tc.profile.Username, tc.password, true)
		})
	}
}

func verifyRepoAuthenticator(ctx context.Context, t *testing.T, a auth.Authenticator, r repo.Repository, username, password string, want bool) {
	t.Helper()

	got := a.IsValid(ctx, r, username, password)
	assert.Equal(t, want, got, "invalid authenticator result for %v/%v", username, password)
}
