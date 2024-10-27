package cli_test

import (
	"os"
	"path"
	"testing"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/tests/testenv"
	"github.com/stretchr/testify/assert"
)

func TestExportPolicy(t *testing.T) {
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-username=user", "--override-hostname=host")

	// check if we get the default global policy
	var policies_1 map[string]*policy.Policy

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "policy", "export"), &policies_1)

	assert.Equal(t, 1, len(policies_1), "unexpected number of policies")
	assert.Equal(t, policy.DefaultPolicy, policies_1["(global)"], "unexpected policy")

	var policies_2 map[string]*policy.Policy

	// we only have one policy, so exporting all policies should be the same as exporting the global policy explicitly
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "policy", "export", "(global)"), &policies_2)

	assert.Equal(t, 1, len(policies_2), "unexpected number of policies")
	assert.Equal(t, policies_1, policies_2, "unexpected policy")

	// create a new policy
	td := testutil.TempDirectory(t)
	id := "user@host:" + td

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
	var policies_3 map[string]*policy.Policy
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "policy", "export", id), &policies_3)

	assert.Equal(t, 1, len(policies_3), "unexpected number of policies")
	assert.Equal(t, expectedPolicy, policies_3[id], "unexpected policy")

	// specifying a local id should return the same policy
	var policies_4 map[string]*policy.Policy
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "policy", "export", td), &policies_4) // note: td, not id

	assert.Equal(t, 1, len(policies_4), "unexpected number of policies")
	assert.Equal(t, expectedPolicy, policies_4[id], "unexpected policy") // thee key is always the full id however

	// exporting without specifying a policy should return all policies
	var policies_5 map[string]*policy.Policy
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "policy", "export"), &policies_5)

	assert.Equal(t, 2, len(policies_5), "unexpected number of policies")
	assert.Equal(t, expectedPolicies, policies_5, "unexpected policy")

	// sanity check if --to-file works
	exportPath := path.Join(td, "exported.json")

	e.RunAndExpectSuccess(t, "policy", "export", "--to-file", exportPath)
	exportedContent, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("unable to read exported file: %v", err)
	}

	var policies_6 map[string]*policy.Policy
	testutil.MustParseJSONLines(t, []string{string(exportedContent)}, &policies_6)

	assert.Equal(t, expectedPolicies, policies_6, "unexpected policy")

	// should not overwrite existing file
	e.RunAndExpectFailure(t, "policy", "export", "--to-file", exportPath, id)

	// unless --overwrite is passed
	e.RunAndExpectSuccess(t, "policy", "export", "--overwrite", "--to-file", exportPath, id)

	exportedContent, err = os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("unable to read exported file: %v", err)
	}

	var policies_7 map[string]*policy.Policy
	testutil.MustParseJSONLines(t, []string{string(exportedContent)}, &policies_7)

	// we specified id, so only that policy should be exported
	assert.Equal(t, 1, len(policies_7), "unexpected number of policies")
	assert.Equal(t, expectedPolicy, policies_5[id], "unexpected policy")
}
