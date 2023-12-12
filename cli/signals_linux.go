package cli

import (
	"os"
	"syscall"
)

func signalLocalToSignalOS(sig Signal) (*os.Signal, error) {
	var osig os.Signal
	switch sig {
	case SignalDump:
		osig = syscall.SIGUSR1
	case SignalTerminate:
		osig = syscall.SIGTERM
	case SignalInterrupt:
		osig = syscall.SIGINT
	default:
		return nil, ErrInvalidSignal
	}
	return &osig, nil
}
