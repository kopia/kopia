package scheduler_test

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/scheduler"
	"github.com/kopia/kopia/internal/testlogging"
)

var baseTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

func TestScheduler(t *testing.T) {
	ch := make(chan string, 1000)

	// times of upcoming events
	it1 := scheduler.Item{"it1", baseTime.Add(100 * time.Millisecond), reportTriggered(t, ch, "it1")}
	it2a := scheduler.Item{"it2", baseTime.Add(200 * time.Millisecond), reportTriggered(t, ch, "it2")}
	it2b := scheduler.Item{"it2", baseTime.Add(200 * time.Millisecond), reportTriggered(t, ch, "it2")}
	it3a := scheduler.Item{"it3", baseTime.Add(300 * time.Millisecond), reportTriggered(t, ch, "it3")}
	it3b := scheduler.Item{"it3", baseTime.Add(300 * time.Millisecond), reportTriggered(t, ch, "it3")}
	it4 := scheduler.Item{"it4", baseTime.Add(30 * time.Hour), reportTriggered(t, ch, "it4")}

	items := []scheduler.Item{it1, it2a, it2b, it3a, it3b, it4}

	// verify that the item order does not matter by shuffling the items.
	rand.Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })

	ctx := testlogging.Context(t)

	ft := faketime.NewClockTimeWithOffset(baseTime.Sub(clock.Now()))

	refresh := make(chan string)
	s := scheduler.Start(ctx, func(ctx context.Context, now time.Time) []scheduler.Item {
		var result []scheduler.Item

		for _, it := range items {
			if it.NextTime.After(now) {
				result = append(result, it)
			}
		}

		return result
	}, scheduler.Options{
		TimeNow:        ft.NowFunc(),
		Debug:          true,
		RefreshChannel: refresh,
	})

	defer s.Stop()

	// ensure that the first few items are triggered in order.
	require.Equal(t, "it1", <-ch)
	require.Equal(t, "it2", <-ch)
	require.Equal(t, "it2", <-ch)
	require.Equal(t, "it3", <-ch)
	require.Equal(t, "it3", <-ch)

	// it4 is far into the future, make sure nothing is triggered immediately
	select {
	case v := <-ch:
		t.Fatalf("unexpected item: %v", v)

	case <-time.After(1 * time.Second):
	}

	// now change the set of items returned by adding it5 which comes before it4
	it5 := scheduler.Item{"it5", ft.NowFunc()().Add(time.Second), reportTriggered(t, ch, "it5")}
	items = []scheduler.Item{it1, it2a, it2b, it3a, it3b, it4, it5}

	refresh <- "x"

	require.Equal(t, "it5", <-ch)
}

// reportTriggered returns a function that reports the item to the provided channel and logs it.
func reportTriggered(t *testing.T, ch chan string, name string) func() {
	t.Helper()

	return func() {
		ch <- name
	}
}

func TestSchedulerWillTriggerItemsInThePast(t *testing.T) {
	ctx := testlogging.Context(t)

	var cnt atomic.Int32

	s := scheduler.Start(ctx, func(ctx context.Context, now time.Time) []scheduler.Item {
		if v := cnt.Add(1); v <= 3 {
			return []scheduler.Item{{
				Description: "past item",
				NextTime:    now.Add(-time.Hour), // item in the past
				Trigger: func() {
					// do nothing
				},
			}}
		}

		return nil
	}, scheduler.Options{
		TimeNow: clock.Now,
		Debug:   true,
	})

	defer s.Stop()

	// wait for the scheduler to process the past items
	time.Sleep(100 * time.Millisecond)

	// verify that the scheduler processed exactly 3 items
	require.Equal(t, int32(3), cnt.Load())
}

func TestSchedulerRefresh(t *testing.T) {
	ctx := testlogging.Context(t)

	var cnt atomic.Int32

	s := scheduler.Start(ctx, func(ctx context.Context, now time.Time) []scheduler.Item {
		return []scheduler.Item{{
			Description: "test item",
			NextTime:    now.Add(time.Hour),
			Trigger: func() {
				cnt.Add(1)
			},
		}}
	}, scheduler.Options{
		TimeNow: clock.Now,
		Debug:   true,
	})

	defer s.Stop()

	// wait a bit
	time.Sleep(100 * time.Millisecond)

	// verify that no items were triggered
	require.Equal(t, int32(0), cnt.Load())
}

func TestTriggerNames(t *testing.T) {
	require.Equal(t, "no triggers", scheduler.TriggerNames(nil))
	require.Equal(t, "no triggers", scheduler.TriggerNames([]scheduler.Item{}))

	require.Equal(t, "single", scheduler.TriggerNames([]scheduler.Item{
		{Description: "single"},
	}))

	require.Equal(t, "2 triggers: first, second", scheduler.TriggerNames([]scheduler.Item{
		{Description: "first"},
		{Description: "second"},
	}))

	require.Equal(t, "6 triggers: a, b, c, d, e [...]", scheduler.TriggerNames([]scheduler.Item{
		{Description: "a"},
		{Description: "b"},
		{Description: "c"},
		{Description: "d"},
		{Description: "e"},
		{Description: "f"},
	}))
}

func TestRapidSchedulingLoopIssue(t *testing.T) {
	// This test demonstrates the rapid scheduling loop issue where
	// the scheduler gets stuck in a loop of immediate triggers when
	// refresh requests are made and items are scheduled for "now".

	var triggerCount int
	var mu sync.Mutex

	// Create a getItems function that returns an item scheduled for "now"
	getItems := func(ctx context.Context, now time.Time) []scheduler.Item {
		mu.Lock()
		defer mu.Unlock()

		// Return an item that should trigger immediately
		return []scheduler.Item{
			{
				Description: "test trigger",
				NextTime:    now, // This causes immediate triggering
				Trigger: func() {
					mu.Lock()
					triggerCount++
					mu.Unlock()
				},
			},
		}
	}

	// Create a scheduler with a short refresh channel
	refreshChan := make(chan string, 1)

	s := scheduler.Start(context.Background(), getItems, scheduler.Options{
		TimeNow:        clock.Now,
		Debug:          true,
		RefreshChannel: refreshChan,
	})

	defer s.Stop()

	// Wait a moment for the scheduler to start
	time.Sleep(100 * time.Millisecond)

	// Send a refresh request (simulating what happens after snapshot/maintenance)
	refreshChan <- "test refresh"

	// Wait a bit to see if we get rapid triggering
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	count := triggerCount
	mu.Unlock()

	// The issue: we should get only 1-2 triggers, not many
	// But with the current bug, we might get many more due to the rapid loop
	if count > 5 {
		t.Errorf("Got %d triggers, expected <= 5 (indicating rapid scheduling loop)", count)
	} else {
		t.Logf("Got %d triggers, which is reasonable", count)
	}
}
