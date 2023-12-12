package cli

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/pkg/errors"
)

// Signal local representation of a signal.
type Signal int

//nolint:revive
var ErrInvalidSignal = errors.New("invalid signal")

// SignalTerminate terminate the process
// SignalInterrupt interrupt the process possibly with termination
// SignalDump dump debug output without termination.
const (
	SignalTerminate = iota + 1
	SignalInterrupt
	SignalDump
)

func (s Signal) String() string {
	switch s {
	case SignalTerminate:
		return "terminate"
	case SignalDump:
		return "dump"
	case SignalInterrupt:
		return "interrupt"
	}

	return "unknown"
}

func onSig(chn chan bool, sig Signal, f func()) {
	s := make(chan os.Signal, 1)

	osig, err := signalLocalToSignalOS(sig)
	if err != nil {
		fmt.Println("ignoring signal", sig.String())
		return
	}

	signal.Notify(s, *osig)

	go func() {
		// invoke the function when either real or simulated signal is delivered
		select {
		case v := <-chn:
			if !v {
				return
			}

		case <-s:
		}
		f()
	}()
}
