package user_test

import (
	"errors"
	"testing"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/user"
)

func TestUserManager(t *testing.T) {
	var env repotesting.Environment

	ctx := testlogging.Context(t)
	defer env.Setup(t).Close(ctx, t)

	if _, err := user.GetUserProfile(ctx, env.RepositoryWriter, "alice"); !errors.Is(err, user.ErrUserNotFound) {
		t.Fatalf("unexpected error: %v", err)
	}

	must(t, user.SetUserProfile(ctx, env.RepositoryWriter, &user.Profile{
		Username:     "alice",
		PasswordHash: "hahaha",
	}))

	if _, err := user.GetUserProfile(ctx, env.RepositoryWriter, "bob"); !errors.Is(err, user.ErrUserNotFound) {
		t.Fatalf("unexpected error: %v", err)
	}

	a, err := user.GetUserProfile(ctx, env.RepositoryWriter, "alice")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, want := a.PasswordHash, "hahaha"; got != want {
		t.Errorf("unexpected password hash: %v, want %v", got, want)
	}

	must(t, user.SetUserProfile(ctx, env.RepositoryWriter, &user.Profile{
		Username:     "alice",
		PasswordHash: "hehehehe",
	}))

	a, err = user.GetUserProfile(ctx, env.RepositoryWriter, "alice")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, want := a.PasswordHash, "hehehehe"; got != want {
		t.Errorf("unexpected password hash: %v, want %v", got, want)
	}

	err = user.DeleteUserProfile(ctx, env.RepositoryWriter, "alice")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err = user.GetUserProfile(ctx, env.RepositoryWriter, "alice"); !errors.Is(err, user.ErrUserNotFound) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func must(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}
