package fs_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
)

func TestUTCTimestamp(t *testing.T) {
	t0 := time.Date(2022, 1, 2, 3, 4, 5, 6, time.UTC)
	t1 := time.Date(2022, 1, 2, 3, 4, 5, 7, time.UTC)
	ut := fs.UTCTimestampFromTime(t0)

	require.Equal(t, "2022-01-02T03:04:05.000000006Z", ut.Format(time.RFC3339Nano))

	var x, y struct {
		TS fs.UTCTimestamp `json:"myts"`
	}

	x.TS = fs.UTCTimestampFromTime(t0)

	v, err := json.Marshal(x)
	require.NoError(t, err)
	require.JSONEq(t, "{\"myts\":\"2022-01-02T03:04:05.000000006Z\"}", string(v))

	require.NoError(t, json.Unmarshal(v, &y))
	require.Equal(t, x, y)

	require.NoError(t, json.Unmarshal([]byte(`{"myts":"2022-07-10T11:15:22.656077568-07:00"}`), &y))
	require.Equal(t, fs.UTCTimestamp(1657476922656077568), y.TS)
	require.Equal(t, "2022-07-10T18:15:22.656077568Z", y.TS.Format(time.RFC3339Nano))

	require.Less(t, fs.UTCTimestampFromTime(t0), fs.UTCTimestampFromTime(t1))
	require.True(t, fs.UTCTimestampFromTime(t0).Equal(fs.UTCTimestampFromTime(t0)))
	require.False(t, fs.UTCTimestampFromTime(t0).Equal(fs.UTCTimestampFromTime(t1)))
	require.True(t, fs.UTCTimestampFromTime(t0).Before(fs.UTCTimestampFromTime(t1)))
	require.True(t, fs.UTCTimestampFromTime(t1).After(fs.UTCTimestampFromTime(t0)))
	require.Equal(t, time.Duration(1), fs.UTCTimestampFromTime(t1).Sub(fs.UTCTimestampFromTime(t0)))
	require.Equal(t, fs.UTCTimestampFromTime(t1), fs.UTCTimestampFromTime(t0).Add(1))

	require.ErrorContains(t, ut.UnmarshalJSON([]byte("invalid-date")), "unable to unmarshal time")
}
