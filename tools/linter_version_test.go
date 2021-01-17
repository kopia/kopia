package tools_test

import (
	"bufio"
	"os"
	"strings"
	"testing"
)

func TestLinterVersion(t *testing.T) {
	makefileVersion := grepLine(t, "tools.mk", "GOLANGCI_LINT_VERSION=")
	workflowVersion := grepLine(t, "../.github/workflows/lint.yml", "version: v")

	if !strings.HasPrefix(makefileVersion, workflowVersion+".") {
		t.Fatalf("linter version out of sync (makefile %v, workflow %v)", makefileVersion, workflowVersion)
	}
}

// grepLine returns the contents of a line in the provided file that contains the provided prefix.
// the result will have the prefix and any whitespace removed.
func grepLine(t *testing.T, fname, prefix string) string {
	t.Helper()

	f, err := os.Open(fname)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		l := strings.TrimSpace(s.Text())
		if strings.HasPrefix(l, prefix) {
			return strings.TrimPrefix(l, prefix)
		}
	}

	if s.Err() != nil {
		t.Fatal(s.Err())
	}

	t.Fatalf("line starting with %v not found in %v", prefix, fname)

	return ""
}
