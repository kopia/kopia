// +build profiling

package cli

import "github.com/pkg/profile"

var (
	profileDir      = app.Flag("profile-dir", "Write profile to the specified directory").Hidden().String()
	profileCPU      = app.Flag("profile-cpu", "Enable CPU profiling").Hidden().Bool()
	profileMemory   = app.Flag("profile-memory", "Enable memory profiling").Hidden().Int()
	profileBlocking = app.Flag("profile-blocking", "Enable block profiling").Hidden().Bool()
	profileMutex    = app.Flag("profile-mutex", "Enable mutex profiling").Hidden().Bool()
)

// withProfiling runs the given callback with profiling enabled, configured according to command line flags.
func withProfiling(callback func() error) error {
	if *profileDir != "" {
		pp := profile.ProfilePath(*profileDir)
		if *profileMemory > 0 {
			defer profile.Start(pp, profile.MemProfileRate(*profileMemory)).Stop()
		}
		if *profileCPU {
			defer profile.Start(pp, profile.CPUProfile).Stop()
		}
		if *profileBlocking {
			defer profile.Start(pp, profile.BlockProfile).Stop()
		}
		if *profileMutex {
			defer profile.Start(pp, profile.MutexProfile).Stop()
		}
	}

	return callback()
}
