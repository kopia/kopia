package endtoend_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/tests/testenv"
)

func TestAutoUpdateEnableTest(t *testing.T) {
	cases := []struct {
		desc             string
		extraArgs        []string
		extraEnv         map[string]string
		wantEnabled      bool
		wantInitialDelay time.Duration
	}{
		{desc: "Default", wantEnabled: true, wantInitialDelay: 24 * time.Hour},
		{desc: "DisabledByFlag", extraArgs: []string{"--no-check-for-updates"}, wantEnabled: false},
		{desc: "DisabledByEnvar-false", extraEnv: map[string]string{"KOPIA_CHECK_FOR_UPDATES": "false"}, wantEnabled: false},
		{desc: "DisabledByEnvar-0", extraEnv: map[string]string{"KOPIA_CHECK_FOR_UPDATES": "0"}, wantEnabled: false},
		{desc: "DisabledByEnvar-f", extraEnv: map[string]string{"KOPIA_CHECK_FOR_UPDATES": "f"}, wantEnabled: false},
		{desc: "DisabledByEnvar-False", extraEnv: map[string]string{"KOPIA_CHECK_FOR_UPDATES": "False"}, wantEnabled: false},
		{desc: "DisabledByEnvar-FALSE", extraEnv: map[string]string{"KOPIA_CHECK_FOR_UPDATES": "FALSE"}, wantEnabled: false},
		{desc: "DisabledByEnvarOverriddenByFlag", extraEnv: map[string]string{"KOPIA_CHECK_FOR_UPDATES": "false"}, extraArgs: []string{"--check-for-updates"}, wantEnabled: true, wantInitialDelay: 24 * time.Hour},
		{desc: "EnabledByEnvarOverriddenByFlag", extraEnv: map[string]string{"KOPIA_CHECK_FOR_UPDATES": "true"}, extraArgs: []string{"--no-check-for-updates"}, wantEnabled: false, wantInitialDelay: 24 * time.Hour},

		{desc: "InitialUpdateCheckIntervalFlag", extraEnv: map[string]string{"KOPIA_INITIAL_UPDATE_CHECK_DELAY": "1h"}, wantEnabled: true, wantInitialDelay: 1 * time.Hour},
		{desc: "InitialUpdateCheckIntervalEnvar", extraArgs: []string{"--initial-update-check-delay=3h"}, wantEnabled: true, wantInitialDelay: 3 * time.Hour},
	}

	os.Unsetenv("KOPIA_CHECK_FOR_UPDATES")

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			runner := testenv.NewInProcRunner(t)
			e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

			// create repo
			args := append([]string{
				"repo", "create", "filesystem", "--path", e.RepoDir,
			}, tc.extraArgs...)

			for k, v := range tc.extraEnv {
				e.Environment[k] = v
			}

			e.RunAndExpectSuccess(t, args...)

			updateInfoFile := filepath.Join(e.ConfigDir, ".kopia.config.update-info.json")
			_, err := os.Stat(updateInfoFile)
			if got, want := err == nil, tc.wantEnabled; got != want {
				t.Errorf("update check enabled: %v, wanted %v", got, want)
			}

			e.RunAndExpectSuccess(t, "repo", "disconnect")
			if _, err = os.Stat(updateInfoFile); !os.IsNotExist(err) {
				t.Errorf("update info file was not removed.")
			}

			args = append([]string{
				"repo", "connect", "filesystem", "--path", e.RepoDir,
			}, tc.extraArgs...)
			e.RunAndExpectSuccess(t, args...)

			// make sure connect behaves the same way as create
			f, err := os.Open(updateInfoFile)
			if got, want := err == nil, tc.wantEnabled; got != want {
				t.Fatalf("update check enabled: %v, wanted %v", got, want)
			}
			if err == nil {
				defer f.Close()

				var state struct {
					NextCheckTime time.Time `json:"nextCheckTimestamp"`
				}

				if err := json.NewDecoder(f).Decode(&state); err != nil {
					t.Fatalf("invalid state JSON: %v", err)
				}

				// verify that initial delay is approximately wantInitialDelay from now +/- 1 minute
				if got, want := clock.Now().Add(tc.wantInitialDelay), state.NextCheckTime; absDuration(got.Sub(want)) > 1*time.Minute {
					t.Errorf("unexpected NextCheckTime: %v, want approx %v", got, want)
				}
			}
		})
	}
}

func absDuration(d time.Duration) time.Duration {
	if d >= 0 {
		return d
	}

	return -d
}
