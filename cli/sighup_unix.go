//go:build !windows

package cli

import (
	"os"
	"os/signal"
	"syscall"
)

// onExternalConfigReloadRequest invokes the provided function when SIGHUP is received.
func onExternalConfigReloadRequest(f func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)

	go func() {
		for {
			<-c
			f()
		}
	}()
}
