package faketime

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/clock"
)

func TestFrozen(t *testing.T) {
	times := []time.Time{
		time.Date(2015, 1, 3, 0, 0, 0, 0, time.UTC),
		clock.Now(),
	}

	for _, tm := range times {
		timeNow := Frozen(tm)

		for range 5 {
			if want, got := tm, timeNow(); got != want {
				t.Fatalf("Invalid frozen time, got: %v, want: %v", got, want)
			}
		}
	}
}

func TestAutoAdvance(t *testing.T) {
	const (
		goRoutinesCount = 3
		iterations      = 20
	)

	startTime := time.Date(2018, 1, 6, 0, 0, 0, 0, time.UTC)
	timeNow := AutoAdvance(startTime, 10*time.Second)
	tchan := make(chan time.Time, 2*goRoutinesCount)

	var wg sync.WaitGroup

	wg.Add(goRoutinesCount)

	for range goRoutinesCount {
		go func() {
			defer wg.Done()

			times := make([]time.Time, iterations)

			for j := range iterations {
				times[j] = timeNow()
			}

			for _, ts := range times {
				tchan <- ts
			}
		}()
	}

	go func() {
		wg.Wait()
		close(tchan)
	}()

	tMap := make(map[time.Time]struct{}, iterations*goRoutinesCount)

	for ts := range tchan {
		if _, ok := tMap[ts]; ok {
			t.Error("Found repeated time value: ", ts)
		}

		tMap[ts] = struct{}{}
	}

	if got, want := len(tMap), goRoutinesCount*iterations; got != want {
		t.Fatalf("number of generated times does not match, got: %v, want: %v", got, want)
	}
}

func TestTimeAdvance(t *testing.T) {
	startTime := time.Date(2019, 1, 6, 0, 0, 0, 0, time.UTC)
	ta := NewTimeAdvance(startTime)
	now := ta.NowFunc()

	if got, want := now(), startTime; got != want {
		t.Errorf("expected time does not match, got: %v, want: %v", got, want)
	}

	dt := 5 * time.Minute
	ta.Advance(dt)

	if got, want := now(), startTime.Add(dt); got != want {
		t.Errorf("expected time does not match, got: %v, want: %v", got, want)
	}
}

func TestTimeAdvanceConcurrent(t *testing.T) {
	const (
		parallelism        = 3
		iterations         = 20
		advanceProbability = 0.3
	)

	startTime := time.Date(2018, 1, 6, 0, 0, 0, 0, time.UTC)
	ta := NewAutoAdvance(startTime, 3*time.Second)
	tchan := make(chan time.Time, 2*parallelism)

	var wg sync.WaitGroup

	wg.Add(parallelism)

	for range parallelism {
		go func() {
			defer wg.Done()

			times := make([]time.Time, iterations)

			var prev time.Time

			for j := range iterations {
				if advanceProbability > rand.Float64() {
					ta.Advance(17 * time.Second)
				}

				times[j] = ta.NowFunc()()

				if times[j].Before(prev) {
					t.Error("Unexpected out-of-order times:", times[j], prev)
				}
			}

			for _, ts := range times {
				tchan <- ts
			}
		}()
	}

	go func() {
		wg.Wait()
		close(tchan)
	}()

	tMap := make(map[time.Time]struct{}, iterations*parallelism)

	for ts := range tchan {
		if _, ok := tMap[ts]; ok {
			t.Error("Found repeated time value: ", ts)
		}

		tMap[ts] = struct{}{}
	}

	if got, want := len(tMap), parallelism*iterations; got != want {
		t.Fatalf("number of generated times does not match, got: %v, want: %v", got, want)
	}
}
