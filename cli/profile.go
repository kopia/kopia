package cli

import (
	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/profile"
)

type profileFlags struct {
	profileDir      string
	profileCPU      bool
	profileMemory   int
	profileBlocking bool
	profileMutex    bool
}

func (c *profileFlags) setup(app *kingpin.Application) {
	app.Flag("profile-dir", "Write profile to the specified directory").Hidden().StringVar(&c.profileDir)
	app.Flag("profile-cpu", "Enable CPU profiling").Hidden().BoolVar(&c.profileCPU)
	app.Flag("profile-memory", "Enable memory profiling").Hidden().IntVar(&c.profileMemory)
	app.Flag("profile-blocking", "Enable block profiling").Hidden().BoolVar(&c.profileBlocking)
	app.Flag("profile-mutex", "Enable mutex profiling").Hidden().BoolVar(&c.profileMutex)
}

// withProfiling runs the given callback with profiling enabled, configured according to command line flags.
func (c *profileFlags) withProfiling(callback func() error) error {
	if c.profileDir != "" {
		pp := profile.ProfilePath(c.profileDir)
		if c.profileMemory > 0 {
			defer profile.Start(pp, profile.MemProfileRate(c.profileMemory)).Stop()
		}

		if c.profileCPU {
			defer profile.Start(pp, profile.CPUProfile).Stop()
		}

		if c.profileBlocking {
			defer profile.Start(pp, profile.BlockProfile).Stop()
		}

		if c.profileMutex {
			defer profile.Start(pp, profile.MutexProfile).Stop()
		}
	}

	return callback()
}
