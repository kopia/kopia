package kopiarunner

import "testing"

func TestSnapListParse(t *testing.T) {
	for _, tc := range []struct {
		name   string
		input  string
		expOut []string
	}{
		{
			name:   "No input",
			input:  "",
			expOut: []string{},
		},
		{
			name:   "test a real input one line",
			input:  "2020-04-02 23:58:40 UTC k1fa28ad0d2df85e76bac85d63d274098 128.5 MB drwxr-xr-x manifest:15d40f2e5af68df27951775ccdef1b60 files:9016 dirs:442 (latest-1,annual-1,monthly-1,weekly-1,daily-1,hourly-1)",
			expOut: []string{"15d40f2e5af68df27951775ccdef1b60"},
		},
		{
			name: "test a real input multiple lines",
			input: `2020-04-02 23:58:40 UTC k1fa28ad0d2df85e76bac85d63d274098 128.5 MB drwxr-xr-x manifest:15d40f2e5af68df27951775ccdef1b60 files:9016 dirs:442 (latest-1,annual-1,monthly-1,weekly-1,daily-1,hourly-1)
2020-04-02 23:58:40 UTC k1fa28ad0d2df85e76bac85d63d274098 128.5 MB drwxr-xr-x manifest:123 files:9016 dirs:442 (latest-1,annual-1,monthly-1,weekly-1,daily-1,hourly-1)
2020-04-02 23:58:40 UTC k1fa28ad0d2df85e76bac85d63d274098 128.5 MB drwxr-xr-x manifest:321 files:9016 dirs:442 (latest-1,annual-1,monthly-1,weekly-1,daily-1,hourly-1)
			`,
			expOut: []string{
				"15d40f2e5af68df27951775ccdef1b60",
				"123",
				"321",
			},
		},
		{
			name:   "Basic input",
			input:  "manifest:asdf",
			expOut: []string{"asdf"},
		},
		{
			name:   "Malformed input",
			input:  "manifust:asdf",
			expOut: []string{},
		},
	} {
		t.Log(tc.name)
		gotOut := parseSnapshotListForSnapshotIDs(tc.input)

		if got, want := len(gotOut), len(tc.expOut); got != want {
			t.Errorf("Output snapshot list length %d does not match expected length %d", got, want)
		}

		for i := range gotOut {
			if got, want := gotOut[i], tc.expOut[i]; got != want {
				t.Errorf("Expected snapshot ID %s but got %s for index %d", want, got, i)
			}
		}
	}
}
