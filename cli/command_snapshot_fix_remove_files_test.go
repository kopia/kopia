package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMatchPathPattern unit-tests the full-path pattern matching used by
// "snapshot fix remove-files --filename".
//
// The case set mirrors the recorded validation matrix in
// kopia_remove_files_validation.md (a local validation transcript kept
// outside the repository), which documents the behavioral difference
// introduced by matchPathPattern(): anchored full-path matching as an
// addition to the original basename-only matching, with "**" behaving
// like "*" (no globstar support).
func TestMatchPathPattern(t *testing.T) {
	cases := []struct {
		pattern   string
		full      string
		wantMatch bool
		wantErr   bool
	}{
		// Basename fallback: a single-segment pattern matches the entry
		// name at any depth (the original path.Match(ent.Name) behavior).
		{".vscode", ".vscode", true, false},
		{".vscode", "src/.vscode", true, false},
		{".vscode", "a/b/.vscode", true, false},
		{".vscode", "notvscode", false, false},

		// Anchored full-path match: the pattern may start at any depth,
		// but from there each segment must match one consecutive segment.
		{"Users/liquid/.vscode", "Users/liquid/.vscode", true, false},
		{"Users/liquid/.vscode", "a/Users/liquid/.vscode", true, false},
		// segments may not be skipped
		{"Users/liquid/.vscode", "Users/liquid/Documents/liquid/.vscode", false, false},
		// "*" spans exactly one path segment
		{"*/.vscode", "docs/.vscode", true, false},
		{"*/.vscode", "a/b/.vscode", true, false},
		// root entries have no parent segment, so a leading "*" cannot match them
		{"*/.vscode", ".vscode", false, false},
		{"liquid/.vscode", "Users/liquid/.vscode", true, false},
		{"liquid/.vscode", "Users/liquid/Documents/liquid/.vscode", true, false},
		// mid-segment wildcard in a multi-segment pattern
		{"Documents/*/.vscode", "Users/liquid/Documents/liquid/.vscode", true, false},
		{"Documents/*/.vscode", "Users/liquid/Documents/github/.vscode", true, false},
		{"Documents/*/.vscode", "Users/liquid/Documents/liquid/x/.vscode", false, false},

		// "**" behaves exactly like "*" (no globstar support)
		{"Users/**/.vscode", "Users/liquid/.vscode", true, false},
		{"Users/**/.vscode", "Users/a/b/.vscode", false, false},

		// a pattern longer than the path can never match
		{"a/b/c", "a/b", false, false},
		{"a/b/c", "a/b/c", true, false},
		{"a/b/c/d", "a/b/c", false, false},

		// a trailing slash yields an empty final segment and never matches
		{"Users/liquid/.vscode/", "Users/liquid/.vscode", false, false},

		// a non-matching pattern is a clean no-op
		{"nonexistent/xyz", "a/b/c", false, false},

		// invalid wildcards surface the stdlib ErrBadPattern
		{"bad[", "bad[", false, true},
		{"bad[-x]", "bad-x", false, true},
	}

	for _, tc := range cases {
		t.Run(tc.pattern+"_"+tc.full, func(t *testing.T) {
			matched, err := matchPathPattern(tc.pattern, tc.full)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantMatch, matched)
		})
	}
}
