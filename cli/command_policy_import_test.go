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

	specifiedPolicies := map[string]*policy.Policy{
		"(global)": policy.DefaultPolicy,
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

	// unknown fields should be disallowed by default
	path_2 := path.Join(td, "policy.json")
	id_2 := "user@host:" + path_2

	err := os.WriteFile(policyFilePath, []byte(`{ "`+id_2+`": { "not-a-real-field": 50 } }`), 0o600)
	specifiedPolicies[id_2] = &policy.Policy{}
	if err != nil {
		t.Fatalf("unable to write policy file: %v", err)
	}

	e.RunAndExpectFailure(t, "policy", "import", "--from-file", policyFilePath, id_2)

	// unless explicitly allowed
	e.RunAndExpectSuccess(t, "policy", "import", "--from-file", policyFilePath, "--allow-unknown-fields", id_2)
	assertPoliciesEqual(t, e, specifiedPolicies) // no change
}

func assertPoliciesEqual(t *testing.T, e *testenv.CLITest, expected map[string]*policy.Policy) {
	var policies map[string]*policy.Policy
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "policy", "export"), &policies)

	assert.Equal(t, expected, policies, "unexpected policies")
}
