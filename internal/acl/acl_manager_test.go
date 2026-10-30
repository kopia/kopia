package acl_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/acl"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

const (
	actualUser      = "bob"
	actualHostname  = "home"
	anotherHostname = "office"

	actualUserAtHostname = actualUser + "@" + actualHostname
)

func TestEffectivePermissions(t *testing.T) {
	cases := []struct {
		entries []*acl.Entry
		target  map[string]string
		want    acl.AccessLevel
	}{
		// no rules
		{
			entries: []*acl.Entry{},
			target:  map[string]string{manifest.TypeLabelKey: snapshot.ManifestType},
			want:    acl.AccessLevelNone,
		},
		{
			entries: []*acl.Entry{},
			target:  map[string]string{manifest.TypeLabelKey: acl.ContentManifestType},
			want:    acl.AccessLevelNone,
		},
		// multiple rules that match subject, highest access wins
		{
			entries: []*acl.Entry{
				{
					Target: acl.TargetRule{manifest.TypeLabelKey: snapshot.ManifestType},
					User:   actualUserAtHostname,
					Access: acl.AccessLevelAppend,
				},
				{
					Target: acl.TargetRule{manifest.TypeLabelKey: snapshot.ManifestType},
					User:   actualUserAtHostname,
					Access: acl.AccessLevelFull,
				},
				{
					Target: acl.TargetRule{manifest.TypeLabelKey: snapshot.ManifestType},
					User:   actualUserAtHostname,
					Access: acl.AccessLevelRead,
				},
			},
			target: map[string]string{manifest.TypeLabelKey: snapshot.ManifestType},
			want:   acl.AccessLevelFull,
		},
		{
			entries: []*acl.Entry{
				{
					Target: acl.TargetRule{manifest.TypeLabelKey: snapshot.ManifestType},
					User:   actualUserAtHostname,
					Access: acl.AccessLevelAppend,
				},
				{
					Target: acl.TargetRule{manifest.TypeLabelKey: policy.ManifestType}, // matches another type
					User:   actualUserAtHostname,
					Access: acl.AccessLevelFull,
				},
				{
					Target: acl.TargetRule{manifest.TypeLabelKey: snapshot.ManifestType},
					User:   actualUserAtHostname,
					Access: acl.AccessLevelRead,
				},
			},
			target: map[string]string{manifest.TypeLabelKey: snapshot.ManifestType},
			want:   acl.AccessLevelAppend,
		},
		{
			entries: []*acl.Entry{
				{
					Target: acl.TargetRule{manifest.TypeLabelKey: snapshot.ManifestType},
					User:   actualUser + "@*", // match any hostname using wildcard
					Access: acl.AccessLevelAppend,
				},
				{
					Target: acl.TargetRule{manifest.TypeLabelKey: snapshot.ManifestType},
					User:   anotherHostname, // matches another user
					Access: acl.AccessLevelFull,
				},
				{
					Target: acl.TargetRule{manifest.TypeLabelKey: snapshot.ManifestType},
					User:   "*@" + actualHostname, // match any username using wildcard
					Access: acl.AccessLevelRead,
				},
			},
			target: map[string]string{manifest.TypeLabelKey: snapshot.ManifestType},
			want:   acl.AccessLevelAppend,
		},
		{
			entries: []*acl.Entry{
				{
					Target: acl.TargetRule{
						manifest.TypeLabelKey: snapshot.ManifestType,
						"foo":                 "bar",
						"u":                   acl.OwnUser,
						"h":                   acl.OwnHost,
					},
					User:   "*@*", // match any user
					Access: acl.AccessLevelAppend,
				},
				{
					Target: acl.TargetRule{
						manifest.TypeLabelKey: snapshot.ManifestType,
						"foo":                 "bar",
						"u":                   "another-user",
						"h":                   acl.OwnHost,
					},
					User:   "*@*", // match any user
					Access: acl.AccessLevelAppend,
				},
			},
			target: map[string]string{
				manifest.TypeLabelKey: snapshot.ManifestType,
				"foo":                 "bar",
				"u":                   actualUser,
				"h":                   actualHostname,
				"extra":               "aaa",
			},
			want: acl.AccessLevelAppend,
		},
	}

	for _, tc := range cases {
		if got := acl.EffectivePermissions(actualUser, actualHostname, tc.target, tc.entries); got != tc.want {
			t.Errorf("invalid access level for entries: %#v and target %#v: %v, want %v", tc.entries, tc.target, got, tc.want)
		}

		// should get same results for filtered entries for the user
		filteredEntries := acl.EntriesForUser(tc.entries, actualUser, actualHostname)

		if got := acl.EffectivePermissions(actualUser, actualHostname, tc.target, filteredEntries); got != tc.want {
			t.Errorf("invalid access level for entries: %#v and target %#v: %v, want %v", tc.entries, tc.target, got, tc.want)
		}
	}
}

func TestLoadEntries(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	// load from nil repository
	entries, err := acl.LoadEntries(ctx, nil, nil)
	require.NoError(t, err)

	if got, want := len(entries), 0; got != want {
		t.Fatalf("invalid number of entries %v, want %v", got, want)
	}

	entries, err = acl.LoadEntries(ctx, env.RepositoryWriter, nil)
	require.NoError(t, err)

	if got, want := len(entries), 0; got != want {
		t.Fatalf("invalid number of entries %v, want %v", got, want)
	}

	e1 := &acl.Entry{
		User: actualUserAtHostname,
		Target: acl.TargetRule{
			manifest.TypeLabelKey: acl.ContentManifestType,
		},
		Access: acl.AccessLevelFull,
	}

	require.NoError(t, acl.AddACL(ctx, env.RepositoryWriter, e1, false))

	entries, err = acl.LoadEntries(ctx, env.RepositoryWriter, entries)
	require.NoError(t, err)

	if got, want := len(entries), 1; got != want {
		t.Fatalf("invalid number of entries %v, want %v", got, want)
	}

	e2 := &acl.Entry{
		User: actualUserAtHostname,
		Target: acl.TargetRule{
			manifest.TypeLabelKey: snapshot.ManifestType,
		},
		Access: acl.AccessLevelFull,
	}

	require.NoError(t, acl.AddACL(ctx, env.RepositoryWriter, e2, false))

	entries, err = acl.LoadEntries(ctx, env.RepositoryWriter, entries)
	require.NoError(t, err)

	if got, want := len(entries), 2; got != want {
		t.Fatalf("invalid number of entries %v, want %v", got, want)
	}
}

func TestACLEntryValidation(t *testing.T) {
	cases := []struct {
		Entry   *acl.Entry
		WantErr string
	}{
		{
			Entry:   nil,
			WantErr: "nil acl",
		},
		{
			Entry: &acl.Entry{
				User: "foo@bar",
				Target: acl.TargetRule{
					"type": "foo",
					"bar":  "baz",
				},
				Access: acl.AccessLevelFull,
			},
			WantErr: "invalid 'type' label, must be one of: acl, content, policy, snapshot, user",
		},
		{
			Entry: &acl.Entry{
				User: "foo@bar",
				Target: acl.TargetRule{
					"type": "snapshot",
					"bar":  "baz",
				},
				Access: acl.AccessLevelFull,
			},
			WantErr: "unsupported label 'bar' for type 'snapshot', must be one of: hostname, path, username",
		},
		{
			Entry: &acl.Entry{
				User: "foo@bar",
				Target: acl.TargetRule{
					"type":     "snapshot",
					"hostname": "",
				},
				Access: acl.AccessLevelFull,
			},
			WantErr: "invalid label 'hostname=' for type 'snapshot': must be non-empty",
		},
		{
			Entry: &acl.Entry{
				User: "foo@bar",
				Target: acl.TargetRule{
					"type":       "policy",
					"policyType": "blah",
				},
				Access: acl.AccessLevelFull,
			},
			WantErr: "invalid label 'policyType=blah' for type 'policy': must be one of: global, host, user, path",
		},
		{
			Entry: &acl.Entry{
				User: "foo@bar",
				Target: acl.TargetRule{
					"type":     "snapshot",
					"hostname": "somehost",
					"username": "someuser",
					"path":     "/",
				},
				Access: acl.AccessLevelFull,
			},
			WantErr: "",
		},
		{
			// valid policy
			Entry: &acl.Entry{
				User: "foo@bar",
				Target: acl.TargetRule{
					"type":       "policy",
					"hostname":   "somehost",
					"username":   "someuser",
					"path":       "/",
					"policyType": "path",
				},
				Access: acl.AccessLevelFull,
			},
			WantErr: "",
		},
		{
			// global policy
			Entry: &acl.Entry{
				User: "foo@bar",
				Target: acl.TargetRule{
					"type":       "policy",
					"policyType": "global",
				},
				Access: acl.AccessLevelFull,
			},
			WantErr: "",
		},
		{
			Entry: &acl.Entry{
				User: "foo@bar",
				Target: acl.TargetRule{
					// ACL that gives access to set ACLs
					"type": "acl",
				},
				Access: acl.AccessLevelFull,
			},
			WantErr: "",
		},
		{
			Entry: &acl.Entry{
				User: "foo@bar",
				Target: acl.TargetRule{
					// ACL that gives access to set other user passwords
					"type":     "user",
					"username": "user@host1",
				},
				Access: acl.AccessLevelFull,
			},
			WantErr: "",
		},
		{
			Entry: &acl.Entry{
				User: "foo@bar@baz",
				Target: acl.TargetRule{
					"type": "snapshot",
				},
				Access: acl.AccessLevelFull,
			},
			WantErr: "user must be 'username@hostname' possibly including wildcards",
		},
		{
			Entry: &acl.Entry{
				User: "foo@bar",
				Target: acl.TargetRule{
					"bar": "baz",
				},
				Access: acl.AccessLevelFull,
			},
			WantErr: "ACL target must have a 'type' label",
		},
		{
			Entry: &acl.Entry{
				User: "foo@bar",
				Target: acl.TargetRule{
					"type": "snapshot",
				},
				Access: -1,
			},
			WantErr: "valid access level must be specified",
		},
	}

	for _, tc := range cases {
		if err := tc.Entry.Validate(); err != nil {
			if got, want := err.Error(), tc.WantErr; got != want {
				t.Fatalf("invalid error %q, want %q", got, want)
			}
		} else if tc.WantErr != "" {
			t.Fatalf("unexpected success, want %q", tc.WantErr)
		}
	}
}
