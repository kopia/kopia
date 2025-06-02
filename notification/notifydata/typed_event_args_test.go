package notifydata_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	apipb "github.com/kopia/kopia/internal/grpcapi"
	"github.com/kopia/kopia/notification/notifydata"
)

func TestUnmarshalEventArgs(t *testing.T) {
	for k := range apipb.NotificationEventArgType_name {
		if k == int32(apipb.NotificationEventArgType_ARG_TYPE_UNKNOWN) {
			continue // Skip the unknown type
		}

		emptyValue, err := notifydata.UnmarshalEventArgs([]byte("{}"), apipb.NotificationEventArgType(k))
		require.NoError(t, err, "unmarshal should not fail for type %v", k)
		testRoundTrip(t, emptyValue)
	}
}

func testRoundTrip(t *testing.T, e notifydata.TypedEventArgs) {
	t.Helper()

	data, err := json.Marshal(e)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	v2, err := notifydata.UnmarshalEventArgs(data, e.EventArgsType())
	require.NoError(t, err)
	require.Equal(t, e, v2)
}
