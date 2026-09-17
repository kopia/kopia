package server_test

import (
	"context"
	"testing"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/servertesting"
	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
	"github.com/stretchr/testify/require"
)

func TestListUsersReturnsEmpty(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	var p = &serverapi.ProfilesResponse{}
	require.NoError(t, cli.Get(ctx, "users", nil, &p))
	require.Len(t, p.Profiles, 0)
}

func TestListUsersReturnsWhenExists(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)
	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, addInitialUser(ctx, env.Repository, "user1@host1"))

	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	var p = &serverapi.ProfilesResponse{}
	require.NoError(t, cli.Get(ctx, "users", nil, &p))
	require.Len(t, p.Profiles, 1)
}

func TestGetReturnsWhenExists(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, addInitialUser(ctx, env.Repository, "user1@host1"))

	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	var p = &serverapi.Profile{}
	require.NoError(t, cli.Get(ctx, "users/"+"user1@host1", nil, &p))

	require.Equal(t, "user1", p.User)
	require.Equal(t, "host1", p.Hostname)
	require.Equal(t, "user1@host1", p.Username)
}

func TestGetReturnsErrorWhenNotExists(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))
	notFound := apiclient.HTTPStatusError{HTTPStatusCode: 404, ErrorMessage: "404 Not Found: user1@host2: user not found"}

	var p = &serverapi.Profile{}
	require.ErrorIs(t, cli.Get(ctx, "users/"+"user1@host2", nil, &p), notFound)
}

func DeleteUserReturnsErrorWhenNotExists(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))
	notFound := apiclient.HTTPStatusError{HTTPStatusCode: 404, ErrorMessage: "404 Not Found: user1@host2: user not found"}

	require.ErrorIs(t, cli.Delete(ctx, "users/"+"user1@host2", nil, nil, &serverapi.Empty{}), notFound)
}

func TestDeleteUserDeletesWhenUserExists(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, addInitialUser(ctx, env.Repository, "user1@host1"))

	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	require.NoError(t, cli.Delete(ctx, "users/"+"user1@host1", nil, nil, &serverapi.Empty{}))

	usr, err := user.GetUserProfile(ctx, env.Repository, "user1@host1")
	require.NotNil(t, err)
	require.Nil(t, usr)

}

func TestAddUserInvalidBodyReturnsError(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	badReq := apiclient.HTTPStatusError{HTTPStatusCode: 400, ErrorMessage: "400 Bad Request: malformed request body"}
	require.ErrorIs(t, cli.Post(ctx, "users", "", badReq), badReq)

}
func TestAddUserInvalidUsernameReturnsError(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	badReq := apiclient.HTTPStatusError{HTTPStatusCode: 400, ErrorMessage: "400 Bad Request: username must be specified as lowercase 'user@hostname'"}
	var r = &serverapi.AddProfileRequest{
		Username: "dummy",
		Password: "dummy",
	}
	require.ErrorIs(t, cli.Post(ctx, "users", r, badReq), badReq)

}
func TestAddUserReturnsErrorWhenExists(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})
	require.NoError(t, addInitialUser(ctx, env.Repository, "user1@host1"))
	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	serverErr := apiclient.HTTPStatusError{HTTPStatusCode: 500, ErrorMessage: "500 Internal Server Error: internal server error: user1@host1: user already exists"}
	var r = &serverapi.AddProfileRequest{
		Username: "user1@host1",
		Password: "dummy",
	}
	require.ErrorIs(t, cli.Post(ctx, "users", r, serverErr), serverErr)
}
func TestAddUserReturnsNewUser(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})
	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	var r = &serverapi.AddProfileRequest{
		Username: "user1@host1",
		Password: "dummy",
	}
	var p = &serverapi.Profile{}

	err = cli.Post(ctx, "users", r, p)
	require.Nil(t, err)
	require.Equal(t, "user1", p.User)
	require.Equal(t, "host1", p.Hostname)
	require.Equal(t, "user1@host1", p.Username)
}

func TestUpdatePasswordInvalidBodyReturnsError(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, addInitialUser(ctx, env.Repository, "user1@host1"))
	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	badReq := apiclient.HTTPStatusError{HTTPStatusCode: 400, ErrorMessage: "400 Bad Request: malformed request body"}
	require.ErrorIs(t, cli.Put(ctx, "users/"+"user1@host1", "", badReq), badReq)
}

func TestUpdatePasswordReturnsErrorWhenNotExists(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})

	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	serverErr := apiclient.HTTPStatusError{HTTPStatusCode: 404, ErrorMessage: "404 Not Found: user1@host1: user not found"}
	var r = &serverapi.UpdateProfilePasswordRequest{
		Password: "dummy",
	}
	require.ErrorIs(t, cli.Put(ctx, "users/"+"user1@host1", r, serverErr), serverErr)
}

func TestUpdatePasswordReturns(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	srvInfo := servertesting.StartServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            servertesting.TestUIUsername,
		Password:                            servertesting.TestUIPassword,
	})
	require.NoError(t, addInitialUser(ctx, env.Repository, "user1@host1"))
	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	var r = &serverapi.UpdateProfilePasswordRequest{
		Password: "dummy",
	}
	var res = &serverapi.Profile{}
	require.NoError(t, cli.Put(ctx, "users/"+"user1@host1", r, res))
	require.Equal(t, "user1", res.User)
	require.Equal(t, "host1", res.Hostname)
	require.Equal(t, "user1@host1", res.Username)
}

func addInitialUser(ctx context.Context, rp repo.Repository, username string) error {
	return repo.WriteSession(ctx, rp, repo.WriteSessionOptions{},
		func(ctx context.Context, w repo.RepositoryWriter) error {
			testProfile := user.Profile{
				Username:            username,
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
		})
}
