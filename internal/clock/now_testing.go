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
var Now = time.Now // nolint:forbidigo

func init() {
	fakeTimeServer := os.Getenv("KOPIA_FAKE_CLOCK_ENDPOINT")
	if fakeTimeServer == "" {
		return
	}

	Now = getTimeFromServer(fakeTimeServer)
}

// getTimeFromServer returns a function that will return timestamp as returned by the server
// increasing it client-side by certain inteval until maximum is reached, at which point
// it will ask the server again for new timestamp.
//
// The server endpoint must be HTTP and be set using KOPIA_FAKE_CLOCK_ENDPOINT environment
// variable.
func getTimeFromServer(endpoint string) func() time.Time {
	var mu sync.Mutex

	var timeInfo struct {
		Next  int64 `json:"next"`
		Step  int64 `json:"step"`
		Until int64 `json:"until"`
	}

	nextRefreshRealTime := time.Now() // nolint:forbidigo

	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()

		if timeInfo.Next >= timeInfo.Until || time.Now().After(nextRefreshRealTime) { // nolint:forbidigo
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

			nextRefreshRealTime = time.Now().Add(refreshServerTimeEvery) // nolint:forbidigo
		}

		v := timeInfo.Next
		timeInfo.Next += timeInfo.Step

		n := time.Unix(0, v)

		return n
	}
}
