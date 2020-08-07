package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/snapshot"
)

func TestPolicyManagerInheritanceTest(t *testing.T) {
	ctx := context.Background()

	var env repotesting.Environment

	defer env.Setup(t).Close(ctx, t)

	defaultPolicyWithLabels := policyWithLabels(DefaultPolicy, map[string]string{
		"policyType": "global",
		"type":       "policy",
	})

	must(t, SetPolicy(ctx, env.Repository, snapshot.SourceInfo{
		Host: "host-a",
	}, &Policy{
		RetentionPolicy: RetentionPolicy{
			KeepDaily: intPtr(44),
		},
	}))

	must(t, SetPolicy(ctx, env.Repository, snapshot.SourceInfo{
		Host:     "host-a",
		UserName: "myuser",
		Path:     "/some/path2",
	}, &Policy{
		RetentionPolicy: RetentionPolicy{
			KeepDaily: intPtr(66),
		},
	}))

	must(t, SetPolicy(ctx, env.Repository, snapshot.SourceInfo{
		Host: "host-b",
	}, &Policy{
		RetentionPolicy: RetentionPolicy{
			KeepDaily: intPtr(55),
		},
	}))

	cases := []struct {
		sourceInfo    snapshot.SourceInfo
		wantEffective *Policy
		wantSources   []snapshot.SourceInfo
	}{
		{
			sourceInfo:    GlobalPolicySourceInfo,
			wantEffective: defaultPolicyWithLabels,
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
		},
		{
			sourceInfo: snapshot.SourceInfo{
				UserName: "myuser",
				Host:     "host-a",
				Path:     "/some/path",
			},
			wantEffective: policyWithLabels(defaultPolicyWithKeepDaily(t, 44), map[string]string{
				"type":       "policy",
				"policyType": "path",
				"hostname":   "host-a",
				"path":       "/some/path",
				"username":   "myuser",
			}),
			wantSources: []snapshot.SourceInfo{
				{Host: "host-a"},
			},
		},
		{
			sourceInfo: snapshot.SourceInfo{
				UserName: "myuser",
				Host:     "host-a",
				Path:     "/some/path2/nested",
			},
			wantEffective: policyWithLabels(defaultPolicyWithKeepDaily(t, 66), map[string]string{
				"type":       "policy",
				"policyType": "path",
				"hostname":   "host-a",
				"path":       "/some/path2/nested",
				"username":   "myuser",
			}),
			wantSources: []snapshot.SourceInfo{
				{UserName: "myuser", Path: "/some/path2", Host: "host-a"},
				{Host: "host-a"},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(fmt.Sprintf("%v", tc.sourceInfo), func(t *testing.T) {
			pol, src, err := GetEffectivePolicy(ctx, env.Repository, tc.sourceInfo)
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
		})
	}
}

func clonePolicy(t *testing.T, p *Policy) *Policy {
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

func defaultPolicyWithKeepDaily(t *testing.T, keepDaily int) *Policy {
	p := clonePolicy(t, DefaultPolicy)
	p.RetentionPolicy.KeepDaily = &keepDaily

	return p
}

func TestPolicyManagerResolvesConflicts(t *testing.T) {
	ctx := context.Background()

	var env repotesting.Environment

	defer env.Setup(t).Close(ctx, t)

	r1 := env.Repository
	r2 := env.MustOpenAnother(t)
	sourceInfo := GlobalPolicySourceInfo

	must(t, SetPolicy(ctx, r1, sourceInfo, &Policy{
		RetentionPolicy: RetentionPolicy{
			KeepDaily: intPtr(44),
		},
	}))

	must(t, SetPolicy(ctx, r2, sourceInfo, &Policy{
		RetentionPolicy: RetentionPolicy{
			KeepDaily: intPtr(33),
		},
	}))

	must(t, r1.Flush(ctx))
	must(t, r2.Flush(ctx))

	r3 := env.MustOpenAnother(t)

	pi, err := GetDefinedPolicy(ctx, r3, sourceInfo)

	must(t, err)

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
	ctx := testlogging.Context(t)

	var env repotesting.Environment

	defer env.Setup(t).Close(ctx, t)

	setPols := map[snapshot.SourceInfo]*Policy{
		// unix-style path names
		{Host: "host-a"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(0)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(1)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home/users"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(2)},
		},
		// on Unix \ a regular character so the directory name is 'user-with\\backslash'
		{Host: "host-a", UserName: "myuser", Path: "/home/users/user-with\\backslash"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(2)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home/users/user-with\\backslash/x"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(2)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home/users/myuser"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(3)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home/users/myuser/dir1"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(4)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home/users/myuser/dir2"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(5)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home/users/myuser/dir2/a"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(6)},
		},
		{Host: "host-a", UserName: "myuser", Path: "/home/users/myuser2"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(7)},
		},

		// windows-style path names with backslash
		{Host: "host-b"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(0)},
		},
		{Host: "host-b", UserName: "myuser", Path: "C:\\"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(1)},
		},
		{Host: "host-b", UserName: "myuser", Path: "C:\\Users"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(2)},
		},
		{Host: "host-b", UserName: "myuser", Path: "C:\\Users\\myuser"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(3)},
		},
		{Host: "host-b", UserName: "myuser", Path: "C:\\Users\\myuser\\dir1"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(4)},
		},
		{Host: "host-b", UserName: "myuser", Path: "C:\\Users\\myuser\\dir2"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(5)},
		},
		{Host: "host-b", UserName: "myuser", Path: "C:\\Users\\myuser\\dir2\\a"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(6)},
		},
		{Host: "host-b", UserName: "myuser", Path: "C:\\Users\\myuser2"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(7)},
		},

		// windows-style path names with slashes
		{Host: "host-c"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(0)},
		},
		{Host: "host-c", UserName: "myuser", Path: "C:/Users"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(1)},
		},
		{Host: "host-c", UserName: "myuser", Path: "C:/Users"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(2)},
		},
		{Host: "host-c", UserName: "myuser", Path: "C:/Users/myuser"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(3)},
		},
		{Host: "host-c", UserName: "myuser", Path: "C:/Users/myuser/dir1"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(4)},
		},
		{Host: "host-c", UserName: "myuser", Path: "C:/Users/myuser/dir2"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(5)},
		},
		{Host: "host-c", UserName: "myuser", Path: "C:/Users/myuser/dir2/a"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(6)},
		},
		{Host: "host-c", UserName: "myuser", Path: "C:/Users/myuser2"}: {
			RetentionPolicy: RetentionPolicy{KeepDaily: intPtr(7)},
		},
	}

	for si, pol := range setPols {
		must(t, SetPolicy(ctx, env.Repository, si, pol))
	}

	cases := []struct {
		si        snapshot.SourceInfo
		wantPaths []string
	}{
		{snapshot.SourceInfo{Host: "host-a", UserName: "myuser", Path: "/tmp"}, []string{"."}},
		{snapshot.SourceInfo{Host: "host-a", UserName: "myuser", Path: "/home/users/myuser"},
			[]string{".", "./dir1", "./dir2", "./dir2/a"},
		},
		{snapshot.SourceInfo{Host: "host-a", UserName: "myuser", Path: "/home/users/myuser2"},
			[]string{"."},
		},
		{snapshot.SourceInfo{Host: "host-a", UserName: "myuser", Path: "/home/users/user-with\\backslash"},
			[]string{".", "./x"},
		},
		{snapshot.SourceInfo{Host: "host-a", UserName: "myuser", Path: "/home"},
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
		{snapshot.SourceInfo{Host: "host-a", UserName: "myuser", Path: "/"},
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
		{snapshot.SourceInfo{Host: "host-b", UserName: "myuser", Path: "C:\\Users\\myuser"},
			[]string{".", "./dir1", "./dir2", "./dir2/a"},
		},
		{snapshot.SourceInfo{Host: "host-b", UserName: "myuser", Path: "C:\\"},
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
		tc := tc
		t.Run(fmt.Sprintf("%v", tc.si), func(t *testing.T) {
			res, err := applicablePoliciesForSource(ctx, env.Repository, tc.si)
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

func must(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}
