package cli_test

import (
	"encoding/json"
	"os"
	"path"
	"testing"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/tests/testenv"
	"github.com/stretchr/testify/assert"
)

// dependent on policy export working
func TestImportPolicy(t *testing.T) {
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-username=user", "--override-hostname=host")

	td := testutil.TempDirectory(t)
	policyFilePath := path.Join(td, "policy.json")

	// poor man's deep copy
	defaultPolicyJson, err := json.Marshal(policy.DefaultPolicy)
	if err != nil {
		t.Fatalf("unable to marshal policy: %v", err)
	}
	var defaultPolicy *policy.Policy
	testutil.MustParseJSONLines(t, []string{string(defaultPolicyJson)}, &defaultPolicy)

	specifiedPolicies := map[string]*policy.Policy{
		"(global)": defaultPolicy,
	}
	makePolicyFile := func() {
		data, err := json.Marshal(specifiedPolicies)
		if err != nil {
			t.Fatalf("unable to marshal policy: %v", err)
		}

		err = os.WriteFile(policyFilePath, data, 0o600)
		if err != nil {
			t.Fatalf("unable to write policy file: %v", err)
		}
	}

	// sanity check that we have the default global policy
	assertPoliciesEqual(t, e, specifiedPolicies)

	// change the global policy
	specifiedPolicies["(global)"].SplitterPolicy.Algorithm = "FIXED-4M"
	makePolicyFile()
	e.RunAndExpectSuccess(t, "policy", "import", "--from-file", policyFilePath)
	assertPoliciesEqual(t, e, specifiedPolicies)

	// create a new policy
	id := "user@host:" + td

	specifiedPolicies[id] = &policy.Policy{
		SplitterPolicy: policy.SplitterPolicy{
			Algorithm: "FIXED-8M",
		},
	}
	makePolicyFile()
	e.RunAndExpectSuccess(t, "policy", "import", "--from-file", policyFilePath)
	assertPoliciesEqual(t, e, specifiedPolicies)

	// import from a file specifying changes in both policies but limiting import to only one
	specifiedPolicies["(global)"].CompressionPolicy.CompressorName = "zstd"
	specifiedPolicies[id].CompressionPolicy.CompressorName = "gzip"
	makePolicyFile()
	e.RunAndExpectSuccess(t, "policy", "import", "--from-file", policyFilePath, "(global)")

	// local policy should not have changed
	specifiedPolicies[id].CompressionPolicy.CompressorName = ""
	assertPoliciesEqual(t, e, specifiedPolicies)

	specifiedPolicies[id].CompressionPolicy.CompressorName = "gzip"
	e.RunAndExpectSuccess(t, "policy", "import", "--from-file", policyFilePath, id)
	assertPoliciesEqual(t, e, specifiedPolicies)

	// deleting values should work
	specifiedPolicies[id].CompressionPolicy.CompressorName = ""
	makePolicyFile()
	e.RunAndExpectSuccess(t, "policy", "import", "--from-file", policyFilePath, id)
	assertPoliciesEqual(t, e, specifiedPolicies)

	// create a new policy
	path_2 := path.Join(td, "policy.json")
	id_2 := "user@host:" + path_2
	policy_2 := &policy.Policy{
		MetadataCompressionPolicy: policy.MetadataCompressionPolicy{
			CompressorName: "zstd",
		},
	}
	specifiedPolicies[id_2] = policy_2
	makePolicyFile()
	e.RunAndExpectSuccess(t, "policy", "import", "--from-file", policyFilePath, id_2)
	assertPoliciesEqual(t, e, specifiedPolicies)

	// unknown fields should be disallowed by default
	err = os.WriteFile(policyFilePath, []byte(`{ "`+id_2+`": { "not-a-real-field": 50, "metadataCompression": { "compressorName": "zstd" } } }`), 0o600)
	if err != nil {
		t.Fatalf("unable to write policy file: %v", err)
	}

	e.RunAndExpectFailure(t, "policy", "import", "--from-file", policyFilePath, id_2)

	// unless explicitly allowed
	e.RunAndExpectSuccess(t, "policy", "import", "--from-file", policyFilePath, "--allow-unknown-fields", id_2)
	assertPoliciesEqual(t, e, specifiedPolicies) // no change

	// deleteOtherPolicies should work
	delete(specifiedPolicies, id_2)
	makePolicyFile()
	e.RunAndExpectSuccess(t, "policy", "import", "--from-file", policyFilePath, "--delete-other-policies")
	assertPoliciesEqual(t, e, specifiedPolicies)

	// add it back in
	specifiedPolicies[id_2] = policy_2
	makePolicyFile()
	e.RunAndExpectSuccess(t, "policy", "import", "--from-file", policyFilePath)
	assertPoliciesEqual(t, e, specifiedPolicies)

	// deleteOtherPolicies should work with specified targets as well
	// don't change policy file
	e.RunAndExpectSuccess(t, "policy", "import", "--from-file", policyFilePath, "--delete-other-policies", "(global)", id)
	delete(specifiedPolicies, id_2)
	assertPoliciesEqual(t, e, specifiedPolicies)

	// --global should be equivalent to (global)
	specifiedPolicies[id_2] = policy_2
	makePolicyFile()
	e.RunAndExpectSuccess(t, "policy", "import", "--from-file", policyFilePath, "--global")
	delete(specifiedPolicies, id_2) // should NOT have been imported
	assertPoliciesEqual(t, e, specifiedPolicies)

	// sanity check against (global)
	e.RunAndExpectSuccess(t, "policy", "import", "--from-file", policyFilePath, "(global)")
	assertPoliciesEqual(t, e, specifiedPolicies)

	// another sanity check
	e.RunAndExpectSuccess(t, "policy", "import", "--from-file", policyFilePath)
	specifiedPolicies[id_2] = policy_2
	assertPoliciesEqual(t, e, specifiedPolicies)
}

func assertPoliciesEqual(t *testing.T, e *testenv.CLITest, expected map[string]*policy.Policy) {
	var policies map[string]*policy.Policy
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "policy", "export"), &policies)

	assert.Equal(t, expected, policies, "unexpected policies")
}
