package cli_test

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/tests/testenv"
)

func TestExportPolicy(t *testing.T) {
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-username=user", "--override-hostname=host")

	// check if we get the default global policy
	var policies1 map[string]*policy.Policy

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "policy", "export"), &policies1)

	assert.Len(t, policies1, 1, "unexpected number of policies")
	assert.Equal(t, policy.DefaultPolicy, policies1["(global)"], "unexpected policy")

	var policies2 map[string]*policy.Policy

	// we only have one policy, so exporting all policies should be the same as exporting the global policy explicitly
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "policy", "export", "(global)"), &policies2)

	assert.Len(t, policies2, 1, "unexpected number of policies")
	assert.Equal(t, policies1, policies2, "unexpected policy")

	// create a new policy
	td := testutil.TempDirectory(t)
	id := snapshot.SourceInfo{
		Host:     "host",
		UserName: "user",
		Path:     td,
	}.String()

	e.RunAndExpectSuccess(t, "policy", "set", td, "--splitter=FIXED-4M")

	expectedPolicy := &policy.Policy{
		SplitterPolicy: policy.SplitterPolicy{
			Algorithm: "FIXED-4M",
		},
	}
	expectedPolicies := map[string]*policy.Policy{
		"(global)": policy.DefaultPolicy,
		id:         expectedPolicy,
	}

	// check if we get the new policy
	var policies3 map[string]*policy.Policy
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "policy", "export", id), &policies3)

	assert.Len(t, policies3, 1, "unexpected number of policies")
	assert.Equal(t, expectedPolicy, policies3[id], "unexpected policy")

	// specifying a local id should return the same policy
	var policies4 map[string]*policy.Policy
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "policy", "export", td), &policies4) // note: td, not id

	assert.Len(t, policies4, 1, "unexpected number of policies")
	assert.Equal(t, expectedPolicy, policies4[id], "unexpected policy") // thee key is always the full id however

	// exporting without specifying a policy should return all policies
	var policies5 map[string]*policy.Policy
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "policy", "export"), &policies5)

	assert.Len(t, policies5, 2, "unexpected number of policies")
	assert.Equal(t, expectedPolicies, policies5, "unexpected policy")

	// sanity check if --to-file works
	exportPath := path.Join(td, "exported.json")

	e.RunAndExpectSuccess(t, "policy", "export", "--to-file", exportPath)
	exportedContent, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("unable to read exported file: %v", err)
	}

	var policies6 map[string]*policy.Policy
	testutil.MustParseJSONLines(t, []string{string(exportedContent)}, &policies6)

	assert.Equal(t, expectedPolicies, policies6, "unexpected policy")

	// should not overwrite existing file
	e.RunAndExpectFailure(t, "policy", "export", "--to-file", exportPath, id)

	// unless --overwrite is passed
	e.RunAndExpectSuccess(t, "policy", "export", "--overwrite", "--to-file", exportPath, id)

	exportedContent, err = os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("unable to read exported file: %v", err)
	}

	var policies7 map[string]*policy.Policy
	testutil.MustParseJSONLines(t, []string{string(exportedContent)}, &policies7)

	// we specified id, so only that policy should be exported
	assert.Len(t, policies7, 1, "unexpected number of policies")
	assert.Equal(t, expectedPolicy, policies5[id], "unexpected policy")

	// pretty-printed JSON should be different but also correct
	policies8prettyJSON := e.RunAndExpectSuccess(t, "policy", "export", "--json-indent")

	var policies8pretty map[string]*policy.Policy
	testutil.MustParseJSONLines(t, policies8prettyJSON, &policies8pretty)

	policies8JSON := e.RunAndExpectSuccess(t, "policy", "export")
	var policies8 map[string]*policy.Policy
	testutil.MustParseJSONLines(t, policies8JSON, &policies8)

	assert.Equal(t, policies8, policies8pretty, "pretty-printing should not change the content")
	assert.NotEqual(t, policies8JSON, policies8prettyJSON, "pretty-printed JSON should be different")

	// --overwrite and no --to-file should fail
	e.RunAndExpectFailure(t, "policy", "export", "--overwrite")

	// writing to inaccessible file should fail
	e.RunAndExpectFailure(t, "policy", "export", "--to-file", "/not/a/real/file/path")
}
