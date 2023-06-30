package debug

import (
	"testing"

	"fmt"

	"github.com/stretchr/testify/require"
)

func TestDebug_parseProfileConfigs(t *testing.T) {
	tcs := []struct {
		in     string
		key    ProfileName
		expect []string
	}{
		{
			in:  "foo=bar;first=one=1,two=2;second;third",
			key: "first",
			expect: []string{
				"one=1",
				"two=2",
			},
		},
		{
			in:  "foo=bar;first=one=1,two=2;second;third",
			key: "foo",
			expect: []string{
				"bar",
			},
		},
		{
			in:     "foo=bar;first=one=1,two=2;second;third",
			key:    "second",
			expect: nil,
		},
		{
			in:     "foo=bar;first=one=1,two=2;second;third",
			key:    "third",
			expect: nil,
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d %s", i, tc.in), func(t *testing.T) {
			pbs := parseProfileConfigs(1<<10, tc.in)
			pb, ok := pbs[tc.key]
			require.True(t, ok)
			require.NotNil(t, pb)                 // always not nil
			require.Equal(t, 1<<10, pb.buf.Cap()) // bufsize is always 1024
			require.Equal(t, 0, pb.buf.Len())
			require.Equal(t, tc.expect, pb.flags)
		})
	}
}

func TestDebug_newProfileConfigs(t *testing.T) {
	tcs := []struct {
		in     string
		key    string
		expect string
		ok     bool
	}{
		{
			in:     "foo=bar",
			key:    "foo",
			ok:     true,
			expect: "bar",
		},
		{
			in:     "foo=",
			key:    "foo",
			ok:     true,
			expect: "",
		},
		{
			in:     "",
			key:    "foo",
			ok:     false,
			expect: "",
		},
		{
			in:     "foo=bar",
			key:    "bar",
			ok:     false,
			expect: "",
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d %s", i, tc.in), func(t *testing.T) {
			pb := newProfileConfig(1<<10, tc.in)
			require.NotNil(t, pb)                 // always not nil
			require.Equal(t, pb.buf.Cap(), 1<<10) // bufsize is always 1024
			v, ok := pb.GetValue(tc.key)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.expect, v)
		})
	}
}
