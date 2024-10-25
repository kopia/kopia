package jsonext_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/jsonext"
)

type MyStruct struct {
	Timeout jsonext.Duration `json:"timeout"`
}

func TestDurationJSONMarshaling(t *testing.T) {
	ms := MyStruct{Timeout: jsonext.Duration{20*time.Minute + 10*time.Second}}

	b, err := json.Marshal(ms)
	require.NoError(t, err)
	require.Equal(t, `{"timeout":"20m10s"}`, string(b))
}

func TestDurationJSONUnmarshaling(t *testing.T) {
	var ms MyStruct

	in := []byte(`{"timeout":"3h20m10s"}`)

	err := json.Unmarshal(in, &ms)
	require.NoError(t, err)

	want := 3*time.Hour + 20*time.Minute + 10*time.Second
	require.Equal(t, want, ms.Timeout.Duration)
}

func TestDurationJSONUnmarshalingError(t *testing.T) {
	var d jsonext.Duration

	in := []byte(`"bogus"`)

	err := json.Unmarshal(in, &d)
	require.ErrorContains(t, err, "invalid duration")
}
