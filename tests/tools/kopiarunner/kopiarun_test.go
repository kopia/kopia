package kopiarunner

import (
	"os"
	"testing"
)

func TestKopiaRunner(t *testing.T) {
	origEnv := os.Getenv("KOPIA_EXE")
	if origEnv == "" {
		t.Skip("Skipping kopia runner test: 'KOPIA_EXE' is unset")
	}

	defer func() {
		envErr := os.Setenv("KOPIA_EXE", origEnv)
		if envErr != nil {
			t.Fatal("Unable to reset env KOPIA_EXE to original value")
		}
	}()

	for _, tt := range []struct {
		name            string
		exe             string
		args            []string
		expNewRunnerErr bool
		expRunErr       bool
	}{
		{
			name:            "empty exe",
			exe:             "",
			args:            nil,
			expNewRunnerErr: true,
			expRunErr:       false,
		},
		{
			name:            "invalid exe",
			exe:             "not-a-program",
			args:            []string{"some", "arguments"},
			expNewRunnerErr: false,
			expRunErr:       true,
		},
		{
			name:            "kopia exe no args",
			exe:             origEnv,
			args:            []string{""},
			expNewRunnerErr: false,
			expRunErr:       true,
		},
		{
			name:            "kopia exe help",
			exe:             origEnv,
			args:            []string{"--help"},
			expNewRunnerErr: false,
			expRunErr:       false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("KOPIA_EXE", tt.exe)

			runner, err := NewRunner("")
			if (err != nil) != tt.expNewRunnerErr {
				t.Fatalf("Expected NewRunner error: %v, got %v", tt.expNewRunnerErr, err)
			}

			if err != nil {
				return
			}

			defer runner.Cleanup()

			_, _, err = runner.Run(tt.args...)
			if (err != nil) != tt.expRunErr {
				t.Fatalf("Expected Run error: %v, got %v", tt.expRunErr, err)
			}
		})
	}
}
