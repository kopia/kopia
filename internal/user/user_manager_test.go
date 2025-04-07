package user_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/user"
)

func TestUserManager(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	if _, err := user.GetUserProfile(ctx, env.RepositoryWriter, "alice@somehost"); !errors.Is(err, user.ErrUserNotFound) {
		t.Fatalf("unexpected error: %v", err)
	}

	require.NoError(t, user.SetUserProfile(ctx, env.RepositoryWriter, &user.Profile{
		Username:     "alice@somehost",
		PasswordHash: []byte("hahaha"),
	}))

	if _, err := user.GetUserProfile(ctx, env.RepositoryWriter, "bob"); !errors.Is(err, user.ErrUserNotFound) {
		t.Fatalf("unexpected error: %v", err)
	}

	a, err := user.GetUserProfile(ctx, env.RepositoryWriter, "alice@somehost")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, want := string(a.PasswordHash), "hahaha"; got != want {
		t.Errorf("unexpected password hash: %v, want %v", got, want)
	}

	require.NoError(t, user.SetUserProfile(ctx, env.RepositoryWriter, &user.Profile{
		Username:     "alice@somehost",
		PasswordHash: []byte("hehehehe"),
	}))

	a, err = user.GetUserProfile(ctx, env.RepositoryWriter, "alice@somehost")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, want := string(a.PasswordHash), "hehehehe"; got != want {
		t.Errorf("unexpected password hash: %v, want %v", got, want)
	}

	err = user.DeleteUserProfile(ctx, env.RepositoryWriter, "alice@somehost")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err = user.GetUserProfile(ctx, env.RepositoryWriter, "alice@somehost"); !errors.Is(err, user.ErrUserNotFound) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetNewProfile(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	p, err := user.GetNewProfile(ctx, env.RepositoryWriter, "alice@somehost")

	require.NoError(t, err)
	require.NotNil(t, p)

	err = p.SetPassword("badpassword")
	require.NoError(t, err)

	err = user.SetUserProfile(ctx, env.RepositoryWriter, p)
	require.NoError(t, err)

	p, err = user.GetNewProfile(ctx, env.RepositoryWriter, p.Username)
	require.ErrorIs(t, err, user.ErrUserAlreadyExists)
	require.Nil(t, p)

	p, err = user.GetNewProfile(ctx, env.RepositoryWriter, "nonexisting@somehost")
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestValidateUsername_Valid(t *testing.T) {
	cases := []string{
		"foo@bar",
		"foo.foo@bar",
		"foo.foo@bar.bar",
		"foo_foo@bar",
		"foo_foo@bar_bar",
		"foo_foo@bar_bar.baz",
		"foo_foo@bar.bar_baz",
		"foo_foo@bar.bar.baz",
		"foo-foo@bar",
		"foo-foo@bar-bar",
		"foo0@0bar.com",
		"foo@barbar",
		"some_user@barbar",
		"some.user@barbar",
		"some-user@some-host",
		"some-user123@some-host123",
		"0@0",          // probably illegal username and hostname, but we're not rejecting that
		"foo--foo@bar", // probably illegal, but we're not rejecting that
		"foo@bar--bar", // probably illegal, but we're not rejecting that
		"-foo@bar",     // probably illegal username, but we're not rejecting that
		"foo@bar-",     // probably illegal hostname, but we're not rejecting that
		"foo-@bar",     // probably illegal username, but we're not rejecting that
		"foo@-bar",     // probably illegal hostname, but we're not rejecting that
	}

	for _, tc := range cases {
		if err := user.ValidateUsername(tc); err != nil {
			t.Fatalf("unexpected invalid username %q: %v", tc, err)
		}
	}
}

func TestValidateUsername_Invalid(t *testing.T) {
	cases := []string{
		"foo@",
		"Foo@bar", // uppercase not allowed
		"foo@Bar",
		"foo!bar@baz",
		"foo@bar@baz",
		"foo@baz@",
		"@",
		"@bar",
	}

	for _, tc := range cases {
		if user.ValidateUsername(tc) == nil {
			t.Fatalf("username should be invalid %q", tc)
		}
	}
}
