package acl_test

import (
	"testing"

	"github.com/kopia/kopia/internal/acl"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
)

func TestACLManager(t *testing.T) {
	var env repotesting.Environment

	ctx := testlogging.Context(t)
	defer env.Setup(t).Close(ctx, t)

	scope1 := acl.Scope{Username: "user1", Hostname: "host1"}

	a, err := acl.GetACL(ctx, env.RepositoryWriter, scope1)
	must(t, err)

	if got, want := a.ContentAccess, acl.AccessLevelUnspecified; got != want {
		t.Errorf("unexpected content access: %v, want %v", got, want)
	}

	must(t, acl.SetACL(ctx, env.RepositoryWriter, scope1, acl.AccessControlList{
		ContentAccess: acl.AccessLevelView,
	}))

	a, err = acl.GetACL(ctx, env.RepositoryWriter, scope1)
	must(t, err)

	if got, want := a.ContentAccess, acl.AccessLevelView; got != want {
		t.Errorf("unexpected content access: %v, want %v", got, want)
	}

	must(t, acl.SetACL(ctx, env.RepositoryWriter, scope1, acl.AccessControlList{
		ContentAccess: acl.AccessLevelAppend,
	}))

	a, err = acl.GetACL(ctx, env.RepositoryWriter, scope1)
	must(t, err)

	if got, want := a.ContentAccess, acl.AccessLevelAppend; got != want {
		t.Errorf("unexpected content access: %v, want %v", got, want)
	}

	err = acl.DeleteACL(ctx, env.RepositoryWriter, scope1)
	must(t, err)

	a, err = acl.GetACL(ctx, env.RepositoryWriter, scope1)
	must(t, err)

	if got, want := a.ContentAccess, acl.AccessLevelUnspecified; got != want {
		t.Errorf("unexpected content access: %v, want %v", got, want)
	}
}

func must(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}
