package repo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBaseURLToURI(t *testing.T) {
	for _, tc := range []struct {
		name      string
		baseURL   string
		expURI    string
		expErrMsg string
	}{
		{
			name:      "ipv4",
			baseURL:   "https://1.2.3.4:5678",
			expURI:    "1.2.3.4:5678",
			expErrMsg: "",
		},
		{
			name:      "ipv6",
			baseURL:   "https://[2600:1f14:253f:ef00:87b9::10]:51515",
			expURI:    "[2600:1f14:253f:ef00:87b9::10]:51515",
			expErrMsg: "",
		},
		{
			name:      "unix https scheme",
			baseURL:   "unix+https:///tmp/kopia-test606141450/sock",
			expURI:    "unix:/tmp/kopia-test606141450/sock",
			expErrMsg: "",
		},
		{
			name:      "kopia scheme",
			baseURL:   "kopia://a:0",
			expURI:    "a:0",
			expErrMsg: "",
		},
		{
			name:      "unix http scheme is invalid",
			baseURL:   "unix+http:///tmp/kopia-test606141450/sock",
			expURI:    "",
			expErrMsg: "invalid server address",
		},
		{
			name:      "invalid address",
			baseURL:   "a",
			expURI:    "",
			expErrMsg: "invalid server address",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotURI, err := baseURLToURI(tc.baseURL)
			if tc.expErrMsg != "" {
				require.ErrorContains(t, err, tc.expErrMsg)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expURI, gotURI)
		})
	}
}
