package auth_test

import (
	"testing"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
)

func TestACL(t *testing.T) {
	aa := auth.ACLAuthorizer()
	ctx := testlogging.Context(t)

	var env repotesting.Environment

	defer env.Setup(t).Close(ctx, t)

	rep := env.Repository

	ai := aa(ctx, rep, "someuser@hostname")

	if got, want := ai.ContentAccessLevel(), auth.AccessLevelNone; got != want {
		t.Errorf("invalid content access level: %v, want %v", got, want)
	}

	verifyManifestAccessLevel(t, ai, globalPolicyLabels, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, ai, fooAtBarPathPolicy, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, ai, fooAtBazPathPolicy, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, ai, fooAtBarPolicy, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, ai, fooAtBazPolicy, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, ai, barPolicy, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, ai, bazPolicy, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, ai, fooAtBarSnapshot, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, ai, fooAtBazSnapshot, auth.AccessLevelNone)
}
