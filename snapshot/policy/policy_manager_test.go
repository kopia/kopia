package policy

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/snapshot"
)

func TestPolicyManagerInheritanceTest(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	defaultPolicyWithLabels := policyWithLabels(DefaultPolicy, map[string]string{
		"policyType": "global",
		"type":       "policy",
	})

	require.NoError(t, SetPolicy(ctx, env.RepositoryWriter, snapshot.SourceInfo{
		Host: "host-a",
	}, &Policy{
		RetentionPolicy: RetentionPolicy{
			KeepDaily: newOptionalInt(44),
		},
	}))

	require.NoError(t, SetPolicy(ctx, env.RepositoryWriter, snapshot.SourceInfo{
		Host:     "host-a",
		UserName: "myuser",
		Path:     "/some/path2",
	}, &Policy{
		RetentionPolicy: RetentionPolicy{
			KeepMonthly: newOptionalInt(66),
		},
	}))

	require.NoError(t, SetPolicy(ctx, env.RepositoryWriter, snapshot.SourceInfo{
		Host: "host-b",
	}, &Policy{
		RetentionPolicy: RetentionPolicy{
			KeepDaily: newOptionalInt(55),
		},
	}))

	cases := []struct {
		sourceInfo    snapshot.SourceInfo
		wantEffective *Policy
		wantSources   []snapshot.SourceInfo
		wantDef       Definition
	}{
		{
			sourceInfo:    GlobalPolicySourceInfo,
			wantEffective: defaultPolicyWithLabels,
			wantSources: []snapshot.SourceInfo{
				GlobalPolicySourceInfo,
			},
		},
		{
			sourceInfo: snapshot.SourceInfo{
				UserName: "myuser",
				Host:     "host-c",
				Path:     "/some/path",
			},
			wantEffective: policyWithLabels(DefaultPolicy, map[string]string{
				"type":       "policy",
				"policyType": "path",
				"hostname":   "host-c",
				"path":       "/some/path",
				"username":   "myuser",
			}),
			wantSources: []snapshot.SourceInfo{
				{
					UserName: "myuser",
					Host:     "host-c",
					Path:     "/some/path",
				},
			},
		},
		{
			sourceInfo: snapshot.SourceInfo{
				UserName: "myuser",
				Host:     "host-a",
				Path:     "/some/path",
			},
			wantEffective: policyWithLabels(policyWithKeepDaily(t, DefaultPolicy, 44), map[string]string{
				"type":       "policy",
				"policyType": "path",
				"hostname":   "host-a",
				"path":       "/some/path",
				"username":   "myuser",
			}),
			wantSources: []snapshot.SourceInfo{
				{UserName: "myuser", Host: "host-a", Path: "/some/path"},
				{Host: "host-a"},
			},
			wantDef: Definition{
				RetentionPolicy: RetentionPolicyDefinition{
					KeepDaily: snapshot.SourceInfo{Host: "host-a"},
				},
			},
		},
		{
			sourceInfo: snapshot.SourceInfo{
				UserName: "myuser",
				Host:     "host-a",
				Path:     "/some/path2/nested",
			},
			wantEffective: policyWithLabels(policyWithKeepDaily(t, policyWithKeepMonthly(t, DefaultPolicy, 66), 44), map[string]string{
				"type":       "policy",
				"policyType": "path",
				"hostname":   "host-a",
				"path":       "/some/path2/nested",
				"username":   "myuser",
			}),
			wantSources: []snapshot.SourceInfo{
				{UserName: "myuser", Path: "/some/path2/nested", Host: "host-a"},
				{UserName: "myuser", Path: "/some/path2", Host: "host-a"},
				{Host: "host-a"},
			},
			wantDef: Definition{
				RetentionPolicy: RetentionPolicyDefinition{
					KeepDaily:   snapshot.SourceInfo{Host: "host-a"},
					KeepMonthly: snapshot.SourceInfo{Host: "host-a", UserName: "myuser", Path: "/some/path2"},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.sourceInfo.String(), func(t *testing.T) {
			pol, def, src, err := GetEffectivePolicy(ctx, env.RepositoryWriter, tc.sourceInfo)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if diff := cmp.Diff(pol, tc.wantEffective); diff != "" {
				t.Errorf("got: %v", pol)
				t.Errorf("want: %v", tc.wantEffective)
				t.Errorf("invalid effective policy: %v", diff)
			}

			var sources []snapshot.SourceInfo
			for _, s := range src {
				sources = append(sources, s.Target())
			}

			if diff := cmp.Diff(sources, tc.wantSources); diff != "" {
				t.Errorf("got: %v", sources)
				t.Errorf("want: %v", tc.wantSources)
				t.Errorf("invalid sources: %v", diff)
			}

			if diff := cmp.Diff(*def, tc.wantDef); diff != "" {
				t.Errorf("got: %v", def)
				t.Errorf("want: %v", tc.wantDef)
				t.Errorf("invalid definition: %v", diff)
			}
		})
	}
}

func clonePolicy(t *testing.T, p *Policy) *Policy {
	t.Helper()

	j, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("unable to marshal JSON: %v", err.Error())
	}

	p2 := &Policy{}
	json.Unmarshal(j, &p2)

	return p2
}

func policyWithLabels(p *Policy, l map[string]string) *Policy {
	p2 := *p
	p2.Labels = l

	return &p2
}

func policyWithKeepDaily(t *testing.T, base *Policy, keepDaily OptionalInt) *Policy {
	t.Helper()

	p := clonePolicy(t, base)
	p.RetentionPolicy.KeepDaily = &keepDaily

	return p
}

func policyWithKeepMonthly(t *testing.T, base *Policy, keepMonthly OptionalInt) *Policy {
	t.Helper()

	p := clonePolicy(t, base)
	p.RetentionPolicy.KeepMonthly = &keepMonthly

	return p
}

func TestPolicyManagerResolvesConflicts(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	r1 := env.RepositoryWriter
	r2 := env.MustOpenAnother(t)
	sourceInfo := GlobalPolicySourceInfo

	require.NoError(t, SetPolicy(ctx, r1, sourceInfo, &Policy{
		RetentionPolicy: RetentionPolicy{
			KeepDaily: newOptionalInt(44),
		},
	}))

	require.NoError(t, SetPolicy(ctx, r2, sourceInfo, &Policy{
		RetentionPolicy: RetentionPolicy{
			KeepDaily: newOptionalInt(33),
		},
	}))

	require.NoError(t, r1.Flush(ctx))
	require.NoError(t, r2.Flush(ctx))

	r3 := env.MustOpenAnother(t)

	pi, err := GetDefinedPolicy(ctx, r3, sourceInfo)

	require.NoError(t, err)

	if got, want := pi.Target(), sourceInfo; got != want {
		t.Errorf("invalid policy target %v, want %v", got, want)
	}

	if got := *pi.RetentionPolicy.KeepDaily; got != 33 && got != 44 {
		t.Errorf("unexpected policy returned")
	}
}

func TestParentPathOSIndependent(t *testing.T) {
	cases := []struct {
		input, want string
	}{
		{"/", "/"},
		{"/x", "/"},
		{"/x/", "/"},
		{"/x/a", "/x"},
		{"/x/a/", "/x"},
		{"x:\\", "x:\\"},
		{"X:\\Program Files", "X:\\"},
		{"X:\\Program Files\\Blah", "X:\\Program Files"},
		{"X:/Program Files", "X:/"},
		{"X:/Program Files/", "X:/"},
		{"X:/Program Files\\", "X:/"},
		{"X:/Program Files/Blah", "X:/Program Files"},
		{"X:/Program Files/Blah/", "X:/Program Files"},
		{"X:/Program Files/Blah/xxx", "X:/Program Files/Blah"},
	}

	for _, tc := range cases {
		if got, want := getParentPathOSIndependent(tc.input), tc.want; got != want {
			t.Errorf("invalid value of getParentPathOSIndependent(%q): %q, want %q", tc.input, got, want)
		}
	}
}

// TestApplicablePoliciesForSource verifies that when we build a policy tree, we pick the appropriate policies
// defined for the subtree regardless of path style (Unix or Windows).
func TestApplicablePoliciesForSource(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	setPols := map[snapshot.SourceInfo]*Policy{
		// unix-style path names
		{Host: "host-a"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(0)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(1)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home/users"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(2)},
		},
		// on Unix \ a regular character so the directory name is 'user-with\\backslash'
		{Host: "host-a", UserName: "myuser", Path: "/home/users/user-with\\backslash"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(2)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home/users/user-with\\backslash/x"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(2)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home/users/myuser"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(3)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home/users/myuser/dir1"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(4)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home/users/myuser/dir2"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(5)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home/users/myuser/dir2/a"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(6)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home/users/myuser2"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(7)},
		},

		// windows-style path names with backslash
		{Host: "host-b"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(0)},
		},
		{Host: "host-b", UserName: "myuser", Path: "C:\\"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(1)},
		},
		{Host: "host-b", UserName: "myuser", Path: "C:\\Users"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(2)},
		},
		{Host: "host-b", UserName: "myuser", Path: "C:\\Users\\myuser"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(3)},
		},
		{Host: "host-b", UserName: "myuser", Path: "C:\\Users\\myuser\\dir1"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(4)},
		},
		{Host: "host-b", UserName: "myuser", Path: "C:\\Users\\myuser\\dir2"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(5)},
		},
		{Host: "host-b", UserName: "myuser", Path: "C:\\Users\\myuser\\dir2\\a"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(6)},
		},
		{Host: "host-b", UserName: "myuser", Path: "C:\\Users\\myuser2"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(7)},
		},

		// windows-style path names with slashes
		{Host: "host-c"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(0)},
		},
		{Host: "host-c", UserName: "myuser", Path: "C:/Users"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(1)},
		},
		{Host: "host-c", UserName: "myuser", Path: "C:/Users"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(2)},
		},
		{Host: "host-c", UserName: "myuser", Path: "C:/Users/myuser"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(3)},
		},
		{Host: "host-c", UserName: "myuser", Path: "C:/Users/myuser/dir1"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(4)},
		},
		{Host: "host-c", UserName: "myuser", Path: "C:/Users/myuser/dir2"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(5)},
		},
		{Host: "host-c", UserName: "myuser", Path: "C:/Users/myuser/dir2/a"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(6)},
		},
		{Host: "host-c", UserName: "myuser", Path: "C:/Users/myuser2"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: newOptionalInt(7)},
		},
	}

	for si, pol := range setPols {
		require.NoError(t, SetPolicy(ctx, env.RepositoryWriter, si, pol))
	}

	cases := []struct {
		si        snapshot.SourceInfo
		wantPaths []string
	}{
		{snapshot.SourceInfo{Host: "host-a", UserName: "myuser", Path: "/tmp"}, []string{"."}},
		{
			snapshot.SourceInfo{Host: "host-a", UserName: "myuser", Path: "/home/users/myuser"},
			[]string{".", "./dir1", "./dir2", "./dir2/a"},
		},
		{
			snapshot.SourceInfo{Host: "host-a", UserName: "myuser", Path: "/home/users/myuser2"},
			[]string{"."},
		},
		{
			snapshot.SourceInfo{Host: "host-a", UserName: "myuser", Path: "/home/users/user-with\\backslash"},
			[]string{".", "./x"},
		},
		{
			snapshot.SourceInfo{Host: "host-a", UserName: "myuser", Path: "/home"},
			[]string{
				".",
				"./users",
				"./users/myuser",
				"./users/myuser/dir1",
				"./users/myuser/dir2",
				"./users/myuser/dir2/a",
				"./users/myuser2",
				`./users/user-with\backslash`,
				`./users/user-with\backslash/x`,
			},
		},
		{
			snapshot.SourceInfo{Host: "host-a", UserName: "myuser", Path: "/"},
			[]string{
				".",
				"./home",
				"./home/users",
				"./home/users/myuser",
				"./home/users/myuser/dir1",
				"./home/users/myuser/dir2",
				"./home/users/myuser/dir2/a",
				"./home/users/myuser2",
				`./home/users/user-with\backslash`,
				`./home/users/user-with\backslash/x`,
			},
		},

		{snapshot.SourceInfo{Host: "host-b", UserName: "myuser", Path: "C:\\Temp"}, []string{"."}},
		{
			snapshot.SourceInfo{Host: "host-b", UserName: "myuser", Path: "C:\\Users\\myuser"},
			[]string{".", "./dir1", "./dir2", "./dir2/a"},
		},
		{
			snapshot.SourceInfo{Host: "host-b", UserName: "myuser", Path: "C:\\"},
			[]string{
				".",
				"./Users",
				"./Users/myuser",
				"./Users/myuser/dir1",
				"./Users/myuser/dir2",
				"./Users/myuser/dir2/a",
				"./Users/myuser2",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.si.String(), func(t *testing.T) {
			res, err := applicablePoliciesForSource(ctx, env.RepositoryWriter, tc.si, nil)
			if err != nil {
				t.Fatalf("error in applicablePoliciesForSource(%v): %v", tc.si, err)
			}

			var relPaths []string
			for k := range res {
				relPaths = append(relPaths, k)
			}

			sort.Strings(relPaths)

			if diff := cmp.Diff(relPaths, tc.wantPaths); diff != "" {
				t.Errorf("invalid sub-policies %v", diff)
			}
		})
	}
}

func TestPolicySetInvalidPath_Valid(t *testing.T) {
	valid := []string{
		"/",
		"/home",
		"c:\\",
		"D:\\",
		"D:\\foo",
		"D:\\foo/bar\\baz",
	}

	for _, v := range valid {
		require.NoError(t, validatePolicyPath(v), v)
	}
}

func TestPolicySetInvalidPath_Invalid(t *testing.T) {
	valid := []string{
		"/home/",
		"//",
		"\\",
		"c:\\users\\",
		"c:/users/",
		"c:\\users/",
	}

	for _, v := range valid {
		require.Error(t, validatePolicyPath(v), v)
	}
}
