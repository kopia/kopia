package auth_test

import (
	"context"
	"testing"

	"github.com/kopia/kopia/internal/auth"
)

var globalPolicyLabels = map[string]string{
	"type":       "policy",
	"policyType": "global",
}

var fooAtBarPathPolicy = map[string]string{
	"type":       "policy",
	"username":   "foo",
	"hostname":   "bar",
	"path":       "/path",
	"policyType": "path",
}

var fooAtBazPathPolicy = map[string]string{
	"type":       "policy",
	"username":   "foo",
	"hostname":   "baz",
	"path":       "/path",
	"policyType": "path",
}

var fooAtBarSnapshot = map[string]string{
	"type":     "snapshot",
	"username": "foo",
	"hostname": "bar",
	"path":     "/path",
}

var fooAtBazSnapshot = map[string]string{
	"type":     "snapshot",
	"username": "foo",
	"hostname": "baz",
	"path":     "/path",
}

var fooAtBarPolicy = map[string]string{
	"type":       "policy",
	"username":   "foo",
	"hostname":   "bar",
	"policyType": "user",
}

var fooAtBazPolicy = map[string]string{
	"type":       "policy",
	"username":   "foo",
	"hostname":   "baz",
	"policyType": "user",
}

var barPolicy = map[string]string{
	"type":       "policy",
	"hostname":   "bar",
	"policyType": "host",
}

var bazPolicy = map[string]string{
	"type":       "policy",
	"hostname":   "baz",
	"policyType": "host",
}

func TestNoAccess(t *testing.T) {
	na := auth.NoAccess

	if got, want := na.ContentAccessLevel(), auth.AccessLevelNone; got != want {
		t.Errorf("invalid content access level: %v, want %v", got, want)
	}

	verifyManifestAccessLevel(t, na, globalPolicyLabels, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, na, fooAtBarPathPolicy, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, na, fooAtBazPathPolicy, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, na, fooAtBarPolicy, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, na, fooAtBazPolicy, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, na, barPolicy, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, na, bazPolicy, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, na, fooAtBarSnapshot, auth.AccessLevelNone)
	verifyManifestAccessLevel(t, na, fooAtBazSnapshot, auth.AccessLevelNone)
}

func TestLegacyAuthorizer(t *testing.T) {
	cases := []struct {
		usernameAtHost           string
		globalPolicyAccess       auth.AccessLevel
		fooAtBarPathPolicyAccess auth.AccessLevel
		fooAtBazPathPolicyAccess auth.AccessLevel
		fooAtBarPolicyAccess     auth.AccessLevel
		fooAtBazPolicyAccess     auth.AccessLevel
		barPolicyAccess          auth.AccessLevel
		bazPolicyAccess          auth.AccessLevel
		fooAtBarSnapshotAccess   auth.AccessLevel
		fooAtBazSnapshotAccess   auth.AccessLevel
	}{
		{
			usernameAtHost:           "foo@bar",
			globalPolicyAccess:       auth.AccessLevelRead,
			fooAtBarPathPolicyAccess: auth.AccessLevelFull, // full access to own path policies
			fooAtBazPathPolicyAccess: auth.AccessLevelNone,
			fooAtBarPolicyAccess:     auth.AccessLevelFull, // full access to own user policy
			fooAtBazPolicyAccess:     auth.AccessLevelNone,
			barPolicyAccess:          auth.AccessLevelRead, // read access to own host policy
			bazPolicyAccess:          auth.AccessLevelNone,
			fooAtBarSnapshotAccess:   auth.AccessLevelFull, // full access to own snapshot
			fooAtBazSnapshotAccess:   auth.AccessLevelNone,
		},
		{
			usernameAtHost:           "evil@bar",
			globalPolicyAccess:       auth.AccessLevelRead,
			fooAtBarPathPolicyAccess: auth.AccessLevelNone,
			fooAtBazPathPolicyAccess: auth.AccessLevelNone,
			fooAtBarPolicyAccess:     auth.AccessLevelNone,
			fooAtBazPolicyAccess:     auth.AccessLevelNone,
			barPolicyAccess:          auth.AccessLevelRead,
			bazPolicyAccess:          auth.AccessLevelNone,
			fooAtBarSnapshotAccess:   auth.AccessLevelNone,
			fooAtBazSnapshotAccess:   auth.AccessLevelNone,
		},
		{
			usernameAtHost:           "evil@elsewhere",
			globalPolicyAccess:       auth.AccessLevelRead,
			fooAtBarPathPolicyAccess: auth.AccessLevelNone,
			fooAtBazPathPolicyAccess: auth.AccessLevelNone,
			fooAtBarPolicyAccess:     auth.AccessLevelNone,
			fooAtBazPolicyAccess:     auth.AccessLevelNone,
			barPolicyAccess:          auth.AccessLevelNone,
			bazPolicyAccess:          auth.AccessLevelNone,
			fooAtBarSnapshotAccess:   auth.AccessLevelNone,
			fooAtBazSnapshotAccess:   auth.AccessLevelNone,
		},
	}

	ctx := context.Background()

	for _, tc := range cases {
		tc := tc
		t.Run(tc.usernameAtHost, func(t *testing.T) {
			a := auth.LegacyAuthorizerForUser(ctx, nil, tc.usernameAtHost)

			if got, want := a.ContentAccessLevel(), auth.AccessLevelFull; got != want {
				t.Errorf("invalid content access level: %v, want %v", got, want)
			}

			verifyManifestAccessLevel(t, a, globalPolicyLabels, tc.globalPolicyAccess)
			verifyManifestAccessLevel(t, a, fooAtBarPathPolicy, tc.fooAtBarPathPolicyAccess)
			verifyManifestAccessLevel(t, a, fooAtBazPathPolicy, tc.fooAtBazPathPolicyAccess)
			verifyManifestAccessLevel(t, a, fooAtBarPolicy, tc.fooAtBarPolicyAccess)
			verifyManifestAccessLevel(t, a, fooAtBazPolicy, tc.fooAtBazPolicyAccess)
			verifyManifestAccessLevel(t, a, barPolicy, tc.barPolicyAccess)
			verifyManifestAccessLevel(t, a, bazPolicy, tc.bazPolicyAccess)
			verifyManifestAccessLevel(t, a, fooAtBarSnapshot, tc.fooAtBarSnapshotAccess)
			verifyManifestAccessLevel(t, a, fooAtBazSnapshot, tc.fooAtBazSnapshotAccess)
		})
	}
}

func verifyManifestAccessLevel(t *testing.T, a auth.AuthorizationInfo, labels map[string]string, wantLevel auth.AccessLevel) {
	t.Helper()

	if got, want := a.ManifestAccessLevel(labels), wantLevel; got != want {
		t.Errorf("invalid access level to %v: %v, want %v", labels, got, want)
	}
}
