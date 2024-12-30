package notifydata

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
)

func TestNewErrorInfo(t *testing.T) {
	startTime := clock.Now()
	endTime := startTime.Add(2 * time.Second)

	err := errors.New("test error") //nolint:err113
	e := NewErrorInfo("test operation", "test details", startTime, endTime, err)

	require.Equal(t, "test operation", e.Operation)
	require.Equal(t, "test details", e.OperationDetails)
	require.Equal(t, startTime, e.StartTime)
	require.Equal(t, endTime, e.EndTime)
	require.Equal(t, "test error", e.ErrorMessage)
	require.Equal(t, "test error", e.ErrorDetails)

	require.Equal(t, startTime.Truncate(time.Second), e.StartTimestamp())
	require.Equal(t, endTime.Truncate(time.Second), e.EndTimestamp())
	require.Equal(t, 2*time.Second, e.Duration())
}
