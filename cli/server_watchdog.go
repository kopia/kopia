package cli

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type serverWatchdog struct {
	inFlightRequests int32
	inner            http.Handler
	stop             func()
	interval         time.Duration

	mu       sync.Mutex
	deadline time.Time
}

func (d *serverWatchdog) getDeadline() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.deadline
}

func (d *serverWatchdog) setDeadline(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if t.After(d.deadline) {
		d.deadline = t
	}
}

func (d *serverWatchdog) Run() {
	for time.Now().Before(d.getDeadline()) || atomic.LoadInt32(&d.inFlightRequests) > 0 {
		// sleep for no less than 1 second to avoid burning CPU
		sleepTime := time.Until(d.getDeadline())
		if sleepTime < time.Second {
			sleepTime = time.Second
		}

		time.Sleep(sleepTime)
	}

	d.stop()
}

func (d *serverWatchdog) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&d.inFlightRequests, 1)
	defer atomic.AddInt32(&d.inFlightRequests, -1)

	d.setDeadline(time.Now().Add(d.interval))
	d.inner.ServeHTTP(w, r)
}

func startServerWatchdog(h http.Handler, interval time.Duration, stop func()) http.Handler {
	w := &serverWatchdog{
		inner:    h,
		interval: interval,
		deadline: time.Now().Add(interval),
		stop:     stop,
	}
	go w.Run()

	return w
}
