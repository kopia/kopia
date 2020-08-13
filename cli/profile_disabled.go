// +build !profiling

package cli

// withProfiling runs the given callback with profiling enabled, configured according to command line flags.
func withProfiling(callback func() error) error {
	return callback()
}
