package scheduler_test

import (
	"context"
	"math/rand"
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
				"it1",
				now.Add(-100 * time.Millisecond),
				func() {
					t.Logf("zzz %v", v)
				},
			}}
		}

		return nil
	}, scheduler.Options{})

	defer s.Stop() // times of upcoming events

	for cnt.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSchedulerRefresh(t *testing.T) {
	ctx := testlogging.Context(t)

	var cnt atomic.Int32

	refresh := make(chan string)
	triggered := make(chan struct{})

	s := scheduler.Start(ctx, func(ctx context.Context, now time.Time) []scheduler.Item {
		t.Logf("now=%v", now)
		switch cnt.Add(1) {
		case 1:
			return []scheduler.Item{{
				"it1",
				now.Add(time.Hour),
				func() {
					t.Error("this should not happen")
				},
			}}
		case 2:
			return []scheduler.Item{{
				"it1",
				now,
				func() {
					close(triggered)
				},
			}}
		}

		return nil
	}, scheduler.Options{
		RefreshChannel: refresh,
	})

	defer s.Stop() // times of upcoming events

	select {
	case <-time.After(time.Second):
		// nothing
	case <-triggered:
		t.Error("triggered too early")
	}

	refresh <- "x"

	select {
	case <-time.After(time.Second):
		t.Error("did not trigger")
	case <-triggered: // success
	}
}

func TestTriggerNames(t *testing.T) {
	cases := []struct {
		items []scheduler.Item
		want  string
	}{
		{nil, "no triggers"},
		{[]scheduler.Item{{Description: "x"}}, "x"},
		{[]scheduler.Item{{Description: "x"}, {Description: "y"}}, "2 triggers: x, y"},
		{[]scheduler.Item{{Description: "a"}, {Description: "b"}, {Description: "c"}, {Description: "d"}, {Description: "e"}, {Description: "f"}}, "6 triggers: a, b, c, d, e [...]"},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, scheduler.TriggerNames(tc.items))
	}
}
