package retry

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
)

var errRetriable = errors.New("retriable")

func isRetriable(e error) bool {
	return errors.Is(e, errRetriable)
}

func TestRetry(t *testing.T) {
	retryInitialSleepAmount = 10 * time.Millisecond
	retryMaxSleepAmount = 20 * time.Millisecond
	maxAttempts = 3

	cnt := 0

	cases := []struct {
		desc      string
		f         func() (int, error)
		want      int
		wantError error
	}{
		{"success-nil", func() (int, error) { return 0, nil }, 0, nil},
		{"success", func() (int, error) { return 3, nil }, 3, nil},
		{"retriable-succeeds", func() (int, error) {
			cnt++
			if cnt < 2 {
				return 0, errRetriable
			}
			return 4, nil
		}, 4, nil},
		{"retriable-never-succeeds", func() (int, error) { return 0, errRetriable }, 0, errors.New("unable to complete retriable-never-succeeds despite 3 retries")},
	}

	ctx := testlogging.Context(t)

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			got, err := WithExponentialBackoff(ctx, tc.desc, tc.f, isRetriable)
			if (err != nil) != (tc.wantError != nil) {
				t.Errorf("invalid error %q, wanted %q", err, tc.wantError)
			}

			if got != tc.want {
				t.Errorf("invalid value %v, wanted %v", got, tc.want)
			}
		})
	}
}

func TestRetryContextCancel(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	canceledctx, cancel := context.WithCancel(ctx)
	cancel()

	require.ErrorIs(t, context.Canceled, WithExponentialBackoffNoValue(canceledctx, "canceled", func() error {
		return errRetriable
	}, isRetriable))
}
