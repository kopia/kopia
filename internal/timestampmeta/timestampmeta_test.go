package timestampmeta_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/timestampmeta"
)

var (
	timeValue   = time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	storedValue = "1577934245000000000"
)

func TestToMap(t *testing.T) {
	require.Equal(t, map[string]string{
		"aaa": storedValue,
	}, timestampmeta.ToMap(timeValue, "aaa"))

	require.Nil(t, timestampmeta.ToMap(time.Time{}, "aaa"))
}
