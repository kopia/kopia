//go:build testing
// +build testing

package clock

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

const refreshServerTimeEvery = 3 * time.Second

// Now is overridable function that returns current wall clock time.
var Now = func() time.Time {
	return discardMonotonicTime(time.Now()) //nolint:forbidigo
}

func init() {
	fakeTimeServer := os.Getenv("KOPIA_FAKE_CLOCK_ENDPOINT")
	if fakeTimeServer == "" {
		return
	}

	Now = getTimeFromServer(fakeTimeServer)
}

// getTimeFromServer returns a function that will return timestamp as returned by the server
// increasing it client-side by certain interval until maximum is reached, at which point
// it will ask the server again for new timestamp.
//
// The server endpoint must be HTTP and be set using KOPIA_FAKE_CLOCK_ENDPOINT environment
// variable.
func getTimeFromServer(endpoint string) func() time.Time {
	var mu sync.Mutex

	var timeInfo struct {
		Time     time.Time     `json:"time"`
		ValidFor time.Duration `json:"validFor"`
	}

	var (
		nextRefreshRealTime time.Time     //nolint:forbidigo
		localTimeOffset     time.Duration // offset to be added to time.Now() to produce server time
	)

	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()

		localTime := time.Now() //nolint:forbidigo
		if localTime.After(nextRefreshRealTime) {
			resp, err := http.Get(endpoint) //nolint:gosec,noctx
			if err != nil {
				log.Fatalf("unable to get fake time from server: %v", err)
			}
			defer resp.Body.Close() //nolint:errcheck

			if resp.StatusCode != http.StatusOK {
				log.Fatalf("unable to get fake time from server: %v", resp.Status)
			}

			if err := json.NewDecoder(resp.Body).Decode(&timeInfo); err != nil {
				log.Fatalf("invalid time received from fake time server: %v", err)
			}

			nextRefreshRealTime = localTime.Add(timeInfo.ValidFor) //nolint:forbidigo

			// compute offset such that localTime + localTimeOffset == serverTime
			localTimeOffset = timeInfo.Time.Sub(localTime)
		}

		return discardMonotonicTime(localTime.Add(localTimeOffset))
	}
}
