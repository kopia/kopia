package cli

import "testing"

func TestNonNegativeIntValueSet(t *testing.T) {
	cases := []struct {
		input     string
		wantErr   bool
		wantValue int
	}{
		{input: "0", wantErr: false, wantValue: 0},
		{input: "1", wantErr: false, wantValue: 1},
		{input: "16", wantErr: false, wantValue: 16},
		{input: "-1", wantErr: true},
		{input: "-100", wantErr: true},
		{input: "abc", wantErr: true},
		{input: "", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			target := -999
			v := nonNegativeIntVar(&target)

			err := v.Set(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Set(%q) expected error, got nil (target=%d)", tc.input, target)
				}

				return
			}

			if err != nil {
				t.Fatalf("Set(%q) unexpected error: %v", tc.input, err)
			}

			if target != tc.wantValue {
				t.Fatalf("Set(%q) target = %d, want %d", tc.input, target, tc.wantValue)
			}

			if got := v.String(); got != tc.input {
				t.Fatalf("String() = %q, want %q", got, tc.input)
			}
		})
	}
}
