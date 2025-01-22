package jsonencoding_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/jsonencoding"
)

type MyStruct struct {
	Timeout jsonencoding.Duration `json:"timeout"`
}

func TestDurationJSONMarshaling(t *testing.T) {
	ms := MyStruct{Timeout: jsonencoding.Duration{20*time.Minute + 10*time.Second}}

	b, err := json.Marshal(ms)
	require.NoError(t, err)
	require.JSONEq(t, `{"timeout":"20m10s"}`, string(b))
}

func TestDurationJSONUnmarshaling(t *testing.T) {
	var ms MyStruct

	cases := []struct {
		input string
		want  time.Duration
	}{
		{
			input: `{"timeout":"3h20m10s"}`,
			want:  3*time.Hour + 20*time.Minute + 10*time.Second,
		},
		{
			input: `{"timeout":" 2305ns "}`,
			want:  2305 * time.Nanosecond,
		},
		{
			input: `{"timeout":"2305ns"}`,
			want:  2305 * time.Nanosecond,
		},
		{
			input: `{"timeout":"2304"}`,
			want:  2304 * time.Nanosecond,
		},
		{
			input: `{"timeout":"  2_304  "}`,
			want:  2304 * time.Nanosecond,
		},
		{
			input: `{"timeout":"  1_002_304  "}`,
			want:  1_002_304 * time.Nanosecond,
		},
		{
			input: `{"timeout":"1_002_303"}`,
			want:  1_002_303 * time.Nanosecond,
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			err := json.Unmarshal([]byte(tc.input), &ms)

			require.NoError(t, err)
			require.Equal(t, tc.want, ms.Timeout.Duration)
		})
	}
}

func TestDurationJSONUnmarshalingError(t *testing.T) {
	var d jsonencoding.Duration

	in := []byte(`"bogus"`)

	err := json.Unmarshal(in, &d)
	require.ErrorContains(t, err, "invalid duration")
}
