// +build profiling

package cli

import "github.com/pkg/profile"

var (
	profileDir      = app.Flag("profile-dir", "Write profile to the specified directory").Hidden().String()
	profileCPU      = app.Flag("profile-cpu", "Enable CPU profiling").Hidden().Bool()
	profileMemory   = app.Flag("profile-memory", "Enable memory profiling").Hidden().Bool()
	profileBlocking = app.Flag("profile-blocking", "Enable block profiling").Hidden().Bool()
	profileMutex    = app.Flag("profile-mutex", "Enable mutex profiling").Hidden().Bool()
)

// withProfiling runs the given callback with profiling enabled, configured according to command line flags
func withProfiling(callback func() error) error {
	if *profileDir != "" {
		profileOpts := []func(*profile.Profile){
			profile.ProfilePath(*profileDir),
		}

		if *profileMemory {
			profileOpts = append(profileOpts, profile.MemProfile)
		}
		if *profileCPU {
			profileOpts = append(profileOpts, profile.CPUProfile)
		}
		if *profileBlocking {
			profileOpts = append(profileOpts, profile.BlockProfile)
		}
		if *profileMutex {
			profileOpts = append(profileOpts, profile.MutexProfile)
		}

		defer profile.Start(profileOpts...).Stop()
	}

	return callback()
}
