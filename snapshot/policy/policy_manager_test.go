package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/kopia/kopia/internal/repotesting"
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
			}, wantEffective: policyWithLabels(DefaultPolicy, map[string]string{
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
			}, wantEffective: policyWithLabels(defaultPolicyWithKeepDaily(t, 44), map[string]string{
				"type":       "policy",
				"policyType": "path",
				"hostname":   "host-a",
				"path":       "/some/path",
				"username":   "myuser",
			}), wantSources: []snapshot.SourceInfo{
				{Host: "host-a"},
			},
		},
		{
			sourceInfo: snapshot.SourceInfo{
				UserName: "myuser",
				Host:     "host-a",
				Path:     "/some/path2/nested",
			}, wantEffective: policyWithLabels(defaultPolicyWithKeepDaily(t, 66), map[string]string{
				"type":       "policy",
				"policyType": "path",
				"hostname":   "host-a",
				"path":       "/some/path2/nested",
				"username":   "myuser",
			}), wantSources: []snapshot.SourceInfo{
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

func must(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}
