package main

import "testing"

func TestEscapeFlags(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "basic single flag",
			input:    "use -flag to enable",
			expected: "use `-flag` to enable",
		},
		{
			name:     "double dash flag",
			input:    "use --long-flag-name to configure",
			expected: "use `--long-flag-name` to configure",
		},
		{
			name:     "multiple flags",
			input:    "use -a or --bee flags",
			expected: "use `-a` or `--bee` flags",
		},
		{
			name:     "should not match in-place",
			input:    "performs in-place modification",
			expected: "performs in-place modification",
		},
		{
			name:     "flags with numbers and hyphens",
			input:    "use --http2-max-streams or -h2-timeout",
			expected: "use `--http2-max-streams` or `-h2-timeout`",
		},
		{
			name:     "flag at start of string",
			input:    "-flag at start",
			expected: "`-flag` at start",
		},
		{
			name:     "existing backticks",
			input:    "use `--existing` and -new flags",
			expected: "use `--existing` and `-new` flags",
		},
		{
			name:     "multiple spaces before flag",
			input:    "test   -flag with spaces",
			expected: "test   `-flag` with spaces",
		},
		{
			name:     "mixed valid and invalid patterns",
			input:    "test in-place and -valid --flags",
			expected: "test in-place and `-valid` `--flags`",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := escapeFlags(tc.input)
			if got != tc.expected {
				t.Errorf("escapeFlags(%q)\ngot:  %q\nwant: %q", tc.input, got, tc.expected)
			}
		})
	}
}
