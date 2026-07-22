package server

import (
	"sync"
	"time"

	"github.com/kopia/kopia/internal/clock"
)

const (
	loginRateLimitWindow     = 15 * time.Minute
	loginRateLimitMaxFails   = 10
	loginRateLimitPurgeEvery = 5 * time.Minute
)

type loginAttemptBucket struct {
	fails       int
	windowStart time.Time
}

type loginRateLimiter struct {
	mu        sync.Mutex
	attempts  map[string]*loginAttemptBucket
	lastPurge time.Time
}

func newLoginRateLimiter() *loginRateLimiter {
	return &loginRateLimiter{
		attempts:  map[string]*loginAttemptBucket{},
		lastPurge: clock.Now(),
	}
}

func (l *loginRateLimiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := clock.Now()
	l.maybePurgeLocked(now)

	b, ok := l.attempts[key]
	if !ok || now.Sub(b.windowStart) > loginRateLimitWindow {
		return true
	}

	return b.fails < loginRateLimitMaxFails
}

func (l *loginRateLimiter) failure(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := clock.Now()
	l.maybePurgeLocked(now)

	b, ok := l.attempts[key]
	if !ok || now.Sub(b.windowStart) > loginRateLimitWindow {
		l.attempts[key] = &loginAttemptBucket{fails: 1, windowStart: now}
		return
	}

	b.fails++
}

func (l *loginRateLimiter) success(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.attempts, key)
}

func (l *loginRateLimiter) maybePurgeLocked(now time.Time) {
	if now.Sub(l.lastPurge) < loginRateLimitPurgeEvery {
		return
	}

	l.lastPurge = now

	for key, b := range l.attempts {
		if now.Sub(b.windowStart) > loginRateLimitWindow {
			delete(l.attempts, key)
		}
	}
}
