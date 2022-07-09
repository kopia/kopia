package cli

import (
	"context"
	"io"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/releasable"
	"github.com/kopia/kopia/repo/logging"
)

// RunSubcommand executes the subcommand asynchronously in current process
// with flags in an isolated CLI environment and returns standard output and standard error.
func (c *App) RunSubcommand(ctx context.Context, kpapp *kingpin.Application, stdin io.Reader, argsAndFlags []string) (stdout, stderr io.Reader, wait func() error, kill func()) {
	stdoutReader, stdoutWriter := io.Pipe()
	stderrReader, stderrWriter := io.Pipe()

	c.stdinReader = stdin
	c.stdoutWriter = stdoutWriter
	c.stderrWriter = stderrWriter
	c.rootctx = logging.WithLogger(ctx, logging.ToWriter(stderrWriter))
	c.simulatedCtrlC = make(chan bool, 1)

	releasable.Created("simulated-ctrl-c", c.simulatedCtrlC)

	c.Attach(kpapp)

	var exitCode int

	resultErr := make(chan error, 1)

	c.osExit = func(ec int) {
		exitCode = ec
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

		if exitCode != 0 {
			resultErr <- errors.Errorf("exit code %v", exitCode)
			return
		}
	}()

	return stdoutReader, stderrReader, func() error {
			return <-resultErr
		}, func() {
			// deliver simulated Ctrl-C to the app.
			c.simulatedCtrlC <- true
		}
}
