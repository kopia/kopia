package cli

import (
	"context"
	"io"
	"os"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/internal/releasable"
	"github.com/kopia/kopia/repo/logging"
)

// RunSubcommand executes the subcommand asynchronously in current process
// with flags in an isolated CLI environment and returns standard output and standard error.
func (c *App) RunSubcommand(ctx context.Context, kpapp *kingpin.Application, stdin io.Reader, argsAndFlags []string) (stdout, stderr io.Reader, wait func() error, interrupt func(os.Signal)) {
	stdoutReader, stdoutWriter := io.Pipe()
	stderrReader, stderrWriter := io.Pipe()

	c.stdinReader = stdin
	c.stdoutWriter = stdoutWriter
	c.stderrWriter = stderrWriter
	c.rootctx = logging.WithLogger(ctx, logging.ToWriter(stderrWriter))
	c.simulatedCtrlC = make(chan bool, 1)
	c.isInProcessTest = true

	releasable.Created("simulated-ctrl-c", c.simulatedCtrlC)

	c.Attach(kpapp)

	var exitError error

	resultErr := make(chan error, 1)

	c.exitWithError = func(ec error) {
		exitError = ec
	}

	go func() {
		defer func() {
			close(c.simulatedCtrlC)
			releasable.Released("simulated-ctrl-c", c.simulatedCtrlC)
		}()

		defer close(resultErr)
		defer stderrWriter.Close() //nolint:errcheck
		defer stdoutWriter.Close() //nolint:errcheck

		_, err := kpapp.Parse(argsAndFlags)
		if err != nil {
			resultErr <- err
			return
		}

		if exitError != nil {
			resultErr <- exitError
			return
		}
	}()

	return stdoutReader, stderrReader, func() error {
			return <-resultErr
		}, func(_ os.Signal) {
			// deliver simulated Ctrl-C to the app.
			c.simulatedCtrlC <- true
		}
}
