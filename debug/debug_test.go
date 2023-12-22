package debug

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDebug_parseProfileConfigs(t *testing.T) {
	tcs := []struct {
		in     string
		key    ProfileName
		expect []string
	}{
		{
			in:  "foo=bar",
			key: "foo",
			expect: []string{
				"bar",
			},
		},
		{
			in:  "first=one=1",
			key: "first",
			expect: []string{
				"one=1",
			},
		},
		{
			in:  "foo=bar:first=one=1",
			key: "first",
			expect: []string{
				"one=1",
			},
		},
		{
			in:  "foo=bar:first=one=1,two=2",
			key: "first",
			expect: []string{
				"one=1",
				"two=2",
			},
		},
		{
			in:  "foo=bar:first=one=1,two=2:second:third",
			key: "first",
			expect: []string{
				"one=1",
				"two=2",
			},
		},
		{
			in:  "foo=bar:first=one=1,two=2:second:third",
			key: "foo",
			expect: []string{
				"bar",
			},
		},
		{
			in:     "foo=bar:first=one=1,two=2:second:third",
			key:    "second",
			expect: nil,
		},
		{
			in:     "foo=bar:first=one=1,two=2:second:third",
			key:    "third",
			expect: nil,
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d %s", i, tc.in), func(t *testing.T) {
			pbs := parseProfileConfigs(1<<10, tc.in)
			pb, ok := pbs[tc.key] // no negative testing for missing keys (see newProfileConfigs)
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

func TestDebug_DumpPem(t *testing.T) {
	ctx := context.Background()
	wrt := bytes.Buffer{}
	// DumpPem dump a PEM version of the byte slice, bs, into writer, wrt.
	err := DumpPem(ctx, []byte("this is a sample PEM"), "test", &wrt)
	require.Nil(t, err)
	require.Equal(t, "-----BEGIN test-----\ndGhpcyBpcyBhIHNhbXBsZSBQRU0=\n-----END test-----\n\n", wrt.String())
}

func TestDebug_StartProfileBuffers(t *testing.T) {
	// regexp for PEMs
	rx := regexp.MustCompile(`(?s:-{5}BEGIN ([A-Z]+)-{5}.(([A-Za-z0-9/+=]{2,80}.)+)-{5}END ([A-Z]+)-{5})`)

	ctx := context.Background()

	t.Setenv(EnvVarKopiaDebugPprof, "")

	buf := bytes.Buffer{}
	func() {
		pprofConfigs = NewProfileConfigs(&buf)

		StartProfileBuffers(ctx)
		defer StopProfileBuffers(ctx)

		time.Sleep(1 * time.Second)
	}()
	s := buf.String()
	mchsss := rx.FindAllString(s, -1)
	// we need zero ... did not start profiling
	require.Len(t, mchsss, 0)

	t.Setenv(EnvVarKopiaDebugPprof, "block=rate=10:cpu:mutex=10")

	buf = bytes.Buffer{}
	func() {
		pprofConfigs = NewProfileConfigs(&buf)

		StartProfileBuffers(ctx)
		defer StopProfileBuffers(ctx)

		time.Sleep(1 * time.Second)
	}()
	s = buf.String()
	mchsss = rx.FindAllString(s, -1)
	// we need three BLOCK, MUTEX and CPU
	require.Len(t, mchsss, 3)
}
