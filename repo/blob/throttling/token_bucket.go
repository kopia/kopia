package throttling

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("throttling")

type tokenBucket struct {
	name  string
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration)

	mu                sync.Mutex
	lastTime          time.Time
	numTokens         float64
	maxTokens         float64
	addTokensTimeUnit time.Duration
}

func (b *tokenBucket) replenishTokens(now time.Time) {
	if !b.lastTime.IsZero() {
		// add tokens based on passage of time, ensuring we don't exceed maxTokens
		elapsed := now.Sub(b.lastTime)
		addTokens := b.maxTokens * elapsed.Seconds() / b.addTokensTimeUnit.Seconds()

		b.numTokens += addTokens
		if b.numTokens > b.maxTokens {
			b.numTokens = b.maxTokens
		}
	}

	b.lastTime = now
}

func (b *tokenBucket) sleepDurationBeforeTokenAreAvailable(n float64, now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.maxTokens == 0 {
		return 0
	}

	b.replenishTokens(now)

	// consume N tokens.
	b.numTokens -= n

	if b.numTokens >= 0 {
		// tokens are immediately available
		return 0
	}

	return time.Duration(float64(b.addTokensTimeUnit.Nanoseconds()) * (-b.numTokens / b.maxTokens))
}

func (b *tokenBucket) Take(ctx context.Context, n float64) {
	d := b.TakeDuration(ctx, n)
	if d > 0 {
		log(ctx).Debugf("sleeping for %v to refill token bucket %v", d, b.name)
		b.sleep(ctx, d)
	}
}

func (b *tokenBucket) TakeDuration(ctx context.Context, n float64) time.Duration {
	return b.sleepDurationBeforeTokenAreAvailable(n, b.now())
}

func (b *tokenBucket) Return(ctx context.Context, n float64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.numTokens += n
	if b.numTokens > b.maxTokens {
		b.numTokens = b.maxTokens
	}
}

func (b *tokenBucket) SetLimit(maxTokens float64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if maxTokens < 0 {
		return errors.New("limit cannot be negative")
	}

	b.maxTokens = maxTokens
	b.maxTokens = maxTokens

	if b.numTokens > b.maxTokens {
		b.numTokens = b.maxTokens
	}

	return nil
}

func sleepWithContext(ctx context.Context, dur time.Duration) {
	t := time.NewTimer(dur)
	defer t.Stop()

	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func newTokenBucket(name string, initialTokens, maxTokens float64, addTimeUnit time.Duration) *tokenBucket {
	return &tokenBucket{
		name:              name,
		now:               time.Now, //nolint:forbidigo
		sleep:             sleepWithContext,
		numTokens:         initialTokens,
		maxTokens:         maxTokens,
		addTokensTimeUnit: addTimeUnit,
	}
}
