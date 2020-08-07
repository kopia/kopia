package policy

import (
	"fmt"
	"reflect"
	"testing"
)

var (
	defPolicy = &Policy{
		FilesPolicy: FilesPolicy{
			IgnoreRules: []string{"default"},
		},
	}
	policyA = &Policy{
		FilesPolicy: FilesPolicy{
			IgnoreRules: []string{"a"},
		},
	}
	policyB = &Policy{
		FilesPolicy: FilesPolicy{
			IgnoreRules: []string{"b"},
		},
	}
	policyC = &Policy{
		FilesPolicy: FilesPolicy{
			IgnoreRules: []string{"c"},
		},
	}
)

func TestTreeChild(t *testing.T) {
	complexTree := &Tree{
		effective: policyA,
		children: map[string]*Tree{
			"foo": {
				effective: policyB,
				children: map[string]*Tree{
					"xxx": {
						effective: policyC,
					},
					"yyy": {
						effective: policyB,
					},
					"zzz": {
						effective: policyA,
					},
				},
			},
			"bar": {
				effective: policyC,
			},
		},
	}

	cases := []struct {
		n             *Tree
		path          string
		wantPolicy    *Policy
		wantInherited bool
	}{
		{nil, "blah", DefaultPolicy, true},
		{&Tree{effective: policyA}, "blah", policyA, true},
		{complexTree, "", policyA, false},
		{complexTree, "foo", policyB, false},
		{complexTree, "foo/anything", policyB, true},
		{complexTree, "foo/xxx", policyC, false},
		{complexTree, "foo/xxx/child/grand/child", policyC, true},
		{complexTree, "foo/yyy", policyB, false},
		{complexTree, "foo/yyy/child", policyB, true},
		{complexTree, "foo/zzz", policyA, false},
		{complexTree, "foo/zzz/child", policyA, true},
		{complexTree, "bar", policyC, false},
		{complexTree, "bar1", policyA, true},
	}

	for _, tc := range cases {
		verifyTreePolicy(t, tc.n, tc.path, tc.wantPolicy, tc.wantInherited)
	}
}

func TestBuildTree(t *testing.T) {
	n := BuildTree(map[string]*Policy{
		".":              policyA,
		"./foo":          policyB,
		"./bar/baz/bleh": policyC,
	}, defPolicy)

	dumpTree(n, "root")

	verifyTreePolicy(t, n, "", policyA, false)
	verifyTreePolicy(t, n, ".", policyA, false)
	verifyTreePolicy(t, n, "./foo", policyB, false)
	verifyTreePolicy(t, n, "foo/.", policyB, false)
	verifyTreePolicy(t, n, "foo/bar", policyB, true)
	verifyTreePolicy(t, n, "./foo/./././bar", policyB, true)
	verifyTreePolicy(t, n, "not-foo", policyA, true)
	verifyTreePolicy(t, n, "bar", policyA, true)
	verifyTreePolicy(t, n, "bar/./baz", policyA, true)
	verifyTreePolicy(t, n, "bar/baz/bleh/././.", policyC, false)
	verifyTreePolicy(t, n, "bar/baz/bleh/./././x", policyC, true)
}

func verifyTreePolicy(t *testing.T, n *Tree, path string, wantPolicy *Policy, wantInherited bool) {
	t.Helper()

	c := n.Child(path)

	if got, want := c.EffectivePolicy(), wantPolicy; !reflect.DeepEqual(got, want) {
		t.Errorf("invalid policy for %q: %v, want %v", path, got, want)
	}

	if got, want := c.IsInherited(), wantInherited; got != want {
		t.Errorf("invalid child 'inherited' result for %q, got %v want %v", path, got, want)
	}
}

func dumpTree(n *Tree, prefix string) {
	fmt.Println(prefix + ".policy: " + n.effective.FilesPolicy.IgnoreRules[0])

	for cname, cnode := range n.children {
		dumpTree(cnode, prefix+"."+cname)
	}
}
