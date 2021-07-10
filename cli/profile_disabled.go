// +build !profiling

package cli

import "github.com/alecthomas/kingpin"

type profileFlags struct{}

func (c *profileFlags) setup(app *kingpin.Application) {
}

// withProfiling runs the given callback with profiling enabled, configured according to command line flags.
func (c *profileFlags) withProfiling(callback func() error) error {
	return callback()
}
