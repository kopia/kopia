package acl_test

import (
	"testing"

	"github.com/kopia/kopia/internal/acl"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
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
			target:  map[string]string{manifest.TypeLabelKey: "content"},
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
	ctx := testlogging.Context(t)

	var env repotesting.Environment

	env.Setup(t).Close(ctx, t)

	// load from nil repository
	entries, err := acl.LoadEntries(ctx, nil, nil)
	must(t, err)

	if got, want := len(entries), 0; got != want {
		t.Fatalf("invalid number of entries %v, want %v", got, want)
	}

	entries, err = acl.LoadEntries(ctx, env.RepositoryWriter, nil)
	must(t, err)

	if got, want := len(entries), 0; got != want {
		t.Fatalf("invalid number of entries %v, want %v", got, want)
	}

	e1 := &acl.Entry{
		User: actualUserAtHostname,
		Target: acl.TargetRule{
			manifest.TypeLabelKey: "content",
		},
		Access:   acl.AccessLevelFull,
		Priority: 10,
	}

	must(t, acl.AddACL(ctx, env.RepositoryWriter, e1))

	entries, err = acl.LoadEntries(ctx, env.RepositoryWriter, entries)
	must(t, err)

	if got, want := len(entries), 1; got != want {
		t.Fatalf("invalid number of entries %v, want %v", got, want)
	}

	e2 := &acl.Entry{
		User: actualUserAtHostname,
		Target: acl.TargetRule{
			manifest.TypeLabelKey: snapshot.ManifestType,
		},
		Access:   acl.AccessLevelFull,
		Priority: 20,
	}

	must(t, acl.AddACL(ctx, env.RepositoryWriter, e2))

	entries, err = acl.LoadEntries(ctx, env.RepositoryWriter, entries)
	must(t, err)

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
				Access:   acl.AccessLevelFull,
				Priority: 50,
			},
			WantErr: "",
		},
		{
			Entry: &acl.Entry{
				User: "foo@bar@baz",
				Target: acl.TargetRule{
					"type": "foo",
					"bar":  "baz",
				},
				Access:   acl.AccessLevelFull,
				Priority: 50,
			},
			WantErr: "user must be 'username@hostname' possibly including wildcards",
		},
		{
			Entry: &acl.Entry{
				User: "foo@bar",
				Target: acl.TargetRule{
					"bar": "baz",
				},
				Access:   acl.AccessLevelFull,
				Priority: 50,
			},
			WantErr: "ACL target must have a 'type' label",
		},
		{
			Entry: &acl.Entry{
				User: "foo@bar",
				Target: acl.TargetRule{
					"type": "foo",
					"bar":  "baz",
				},
				Access:   -1,
				Priority: 50,
			},
			WantErr: "valid access level must be specified",
		},
		{
			Entry: &acl.Entry{
				User: "foo@bar",
				Target: acl.TargetRule{
					"type": "foo",
					"bar":  "baz",
				},
				Access:   acl.AccessLevelFull,
				Priority: 150,
			},
			WantErr: "invalid priority, must be 1 (highest) to 100 (lowest)",
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
			WantErr: "invalid priority, must be 1 (highest) to 100 (lowest)",
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

func must(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}
