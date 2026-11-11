package repo

import "testing"

func TestNormalizeDefaultUserName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		in      string
		windows bool
		want    string
	}{
		{name: "already lowercase", in: "alice", want: "alice"},
		{name: "mixed case", in: "Alice", want: "alice"},
		{name: "windows mixed case", in: "Alice", windows: true, want: "alice"},
		{name: "windows domain prefix", in: `CORP\Alice`, windows: true, want: "alice"},
		{name: "domain prefix kept off windows", in: `CORP\Alice`, windows: false, want: `corp\alice`},
		{name: "empty", in: "", want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := normalizeDefaultUserName(tc.in, tc.windows)
			if got != tc.want {
				t.Fatalf("normalizeDefaultUserName(%q, windows=%v) = %q, want %q", tc.in, tc.windows, got, tc.want)
			}
		})
	}
}

func TestGetDefaultUserNameIsLowercase(t *testing.T) {
	t.Parallel()
	got := GetDefaultUserName(t.Context())
	if got != "" && got != "nobody" {
		for _, r := range got {
			if r >= 'A' && r <= 'Z' {
				t.Fatalf("GetDefaultUserName() = %q, want lowercase", got)
			}
		}
	}
}
