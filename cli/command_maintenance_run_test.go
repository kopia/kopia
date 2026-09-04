package cli_test

import (
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/logfile"
)

func TestMaintenanceRunReportsInvalidGlobalFlag(t *testing.T) {
	t.Parallel()

	app := cli.NewApp()
	parser := kingpin.New("kopia", "")
	logfile.Attach(app, parser)
	app.Attach(parser)

	_, err := parser.Parse([]string{"--log-level=warn", "maintenance", "run", "--safety=none"})

	require.ErrorContains(t, err, "enum value must be one of debug,info,warning,error, got 'warn'")
}
