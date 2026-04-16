package kopiarunner

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKopiaRunner(t *testing.T) {
	origEnv := os.Getenv("KOPIA_EXE")
	if origEnv == "" {
		t.Skip("Skipping kopia runner test: 'KOPIA_EXE' is unset")
	}

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
			if tt.expNewRunnerErr {
				require.Error(t, err, "expected NewRunner error")

				return
			}

			require.NoError(t, err)

			t.Cleanup(runner.Cleanup)

			_, _, err = runner.Run(tt.args...)
			if tt.expRunErr {
				require.Error(t, err, "expected Run error")

				return
			}

			require.NoError(t, err)
		})
	}
}
