// Package scheduler implements a simple scheduler that triggers the next item when its due time is
// reached based on the list of upcoming items.
package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("scheduler")

// GetItemsFunc is a callback that returns items for the scheduler to consider.
type GetItemsFunc func(ctx context.Context, now time.Time) []Item

// Item describes an item that can be scheduled with a function that is invoked the next time the item is due.
type Item struct {
	Description string
	NextTime    time.Time
	Trigger     func()
}

// Scheduler manages triggering of arbitrary events by periodically determining the first
// of a set of upcoming events and waiting until it's due and invoking the trigger function.
type Scheduler struct {
	TimeNow func() time.Time
	Debug   bool

	refreshRequested chan string
	getItems         GetItemsFunc
	closed           chan struct{}
	wg               sync.WaitGroup
}

// Options the scheduler.
type Options struct {
	TimeNow        func() time.Time
	Debug          bool
	RefreshChannel chan string
}

// Start runs a new scheduler that will call getItems() to get the list of items to schedule.
func Start(ctx context.Context, getItems GetItemsFunc, opts Options) *Scheduler {
	timeNow := opts.TimeNow

	if timeNow == nil {
		timeNow = clock.Now
	}

	s := &Scheduler{
		TimeNow:          timeNow,
		refreshRequested: opts.RefreshChannel,
		closed:           make(chan struct{}),
		getItems:         getItems,
		Debug:            opts.Debug,
	}

	s.wg.Add(1)

	go func() {
		defer s.wg.Done()

		s.run(context.WithoutCancel(ctx))
	}()

	return s
}

const sleepTimeWhenNoUpcomingSnapshots = 8 * time.Hour

func (s *Scheduler) upcomingItems(ctx context.Context, now time.Time) (nextTriggerTime time.Time, toTrigger []Item) {
	allsm := s.getItems(ctx, now)

	for _, t := range allsm {
		if nextTriggerTime.IsZero() || t.NextTime.Before(nextTriggerTime) {
			nextTriggerTime = t.NextTime
			toTrigger = nil
		}

		if t.NextTime.Equal(nextTriggerTime) {
			toTrigger = append(toTrigger, t)
		}
	}

	if s.Debug {
		log(ctx).Debugf(">>>> scheduling %v items (now %v)", len(allsm), now.Format(time.RFC3339))

		for _, it := range allsm {
			log(ctx).Debugf("  %v %v", it.NextTime.Format(time.RFC3339), it.Description)
		}
	}

	return nextTriggerTime, toTrigger
}

func sleepTimeOrDefault(now, t time.Time, def time.Duration) time.Duration {
	if t.IsZero() {
		return def
	}

	// note this may negative if the getItems returns items in the past.
	return t.Sub(now)
}

// Stop stops the scheduler.
func (s *Scheduler) Stop() {
	close(s.closed)
	s.wg.Wait()
}

func (s *Scheduler) run(ctx context.Context) {
	var timer *time.Timer

	for {
		now := s.TimeNow()
		nextTriggerTime, toTrigger := s.upcomingItems(ctx, now)

		sleepTimeUntilNextTrigger := sleepTimeOrDefault(now, nextTriggerTime, sleepTimeWhenNoUpcomingSnapshots)
		if sleepTimeUntilNextTrigger < 0 {
			sleepTimeUntilNextTrigger = 0
		}

		if s.Debug && sleepTimeUntilNextTrigger > 0 {
			log(ctx).Debugf("sleeping for %v until %v (%v)",
				sleepTimeUntilNextTrigger,
				nextTriggerTime.Format(time.RFC3339),
				TriggerNames(toTrigger),
			)
		}

		// stop previous timer, if any
		if timer != nil {
			timer.Stop()
		}

		timer = time.NewTimer(sleepTimeUntilNextTrigger)

		select {
		case <-s.closed:
			// stopping, just exit
			return

		case <-timer.C:
			for _, sm := range toTrigger {
				log(ctx).Debugf("triggering %v", sm.Description)

				sm.Trigger()
			}

		case reason := <-s.refreshRequested:
			if s.Debug {
				log(ctx).Debugw("schedule re-evaluation requested", "reason", reason)
			}
		}
	}
}

// TriggerNames returns a human-readable description of the items that are about to be triggered.
func TriggerNames(toTrigger []Item) string {
	var result []string

	for _, t := range toTrigger {
		result = append(result, t.Description)
	}

	if len(result) == 0 {
		return "no triggers"
	}

	if len(result) == 1 {
		return result[0]
	}

	s := fmt.Sprintf("%v triggers: ", len(result))

	var suffix string

	const maxItems = 5

	if len(result) > maxItems {
		suffix = " [...]"
		result = result[:maxItems]
	}

	return s + strings.Join(result, ", ") + suffix
}
