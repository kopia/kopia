package pproflogging

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"github.com/kopia/kopia/repo/logging"
)

//nolint:gocritic
var pemRegexp = regexp.MustCompile("(?sm:^(-{5}BEGIN ([A-Z]+)-{5}$.)(([A-Za-z0-9/+=]{2,80}$.)+)(^-{5}END ([A-Z]+)-{5})$)")

// TestDebug_StartProfileBuffers test setup of profile buffers with configuration set from the environment.
func TestDebug_StartProfileBuffers(t *testing.T) {
	// save environment and restore after testing
	saveLockEnv(t)

	tcs := []struct {
		inArgs               string
		expectedProfileCount int
		expectKeys           []ProfileName
		expectOptions        [][2]string
		rx                   *regexp.Regexp
	}{
		{
			inArgs:               "",
			expectedProfileCount: 0,
		},
		{
			inArgs:               "block=debug=foo",
			expectedProfileCount: 0,
			expectKeys:           []ProfileName{"block"},
			expectOptions:        [][2]string{{"block", "debug=foo"}},
		},
		{
			inArgs:               "block=rate=10:cpu:mutex=10",
			expectedProfileCount: 3,
			expectKeys:           []ProfileName{"block", "cpu", "mutex"},
			expectOptions:        [][2]string{{"block", "rate=10"}, {"cpu", ""}, {"mutex", "10"}},
		},
		{
			inArgs:               "block=rate=10:nonelikethis=foo",
			expectedProfileCount: 1,
			expectKeys:           []ProfileName{"block", "nonelikethis"},
			expectOptions:        [][2]string{{"block", "rate=10"}, {"nonelikethis", "foo"}},
		},
		{
			inArgs:               "block=rate=10:cpu:mutex=10,debug=1",
			expectedProfileCount: 3,
			expectKeys:           []ProfileName{"block", "cpu", "mutex"},
			expectOptions:        [][2]string{{"block", "rate=10"}, {"cpu", ""}, {"mutex", "10"}, {"mutex", "debug=1"}},
		},
		{
			inArgs:               "block=rate=10,debug=1:cpu:mutex=10",
			expectedProfileCount: 3,
			expectKeys:           []ProfileName{"block", "cpu", "mutex"},
			expectOptions:        [][2]string{{"block", "debug=1"}, {"block", "rate=10"}, {"cpu", ""}, {"mutex", "10"}},
		},
		{
			inArgs: "",
			rx:     regexp.MustCompile("no profile buffers enabled"),
		},
		{
			inArgs: ":",
			rx:     regexp.MustCompile(`cannot start PPROF config, ".*", due to parse error`),
		},
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d: %q", i, tc.inArgs), func(t *testing.T) {
			t.Setenv(EnvVarKopiaDebugPprof, tc.inArgs)

			lg := &bytes.Buffer{}

			// CPU configuration will be removed if its already
			// configured.  This guarantees that the CPU profile is reset
			pprof.StopCPUProfile()
			// initialize pprofConfigs to initial value
			pprofConfigs = newProfileConfigs(nil)

			ctx := logging.WithLogger(context.Background(), logging.ToWriter(lg))

			StartProfileBuffers(ctx)

			// grab lock for rest of test
			pprofConfigs.mu.Lock()
			defer pprofConfigs.mu.Unlock()

			require.Equal(t, len(tc.expectKeys), len(pprofConfigs.pcm))

			for _, nm := range tc.expectKeys {
				_, ok := pprofConfigs.pcm[nm]
				require.True(t, ok)
			}

			for _, pth := range tc.expectOptions {
				v0, ok := pprofConfigs.pcm[ProfileName(pth[0])]
				require.True(t, ok)
				if pth[1] == "" && len(v0.flags) == 0 {
					continue
				}
				require.Contains(t, v0.flags, pth[1])
			}

			if tc.rx != nil {
				require.Regexp(t, tc.rx, lg.String())
			}

			time.Sleep(1 * time.Second)
		})
	}
}

func TestDebug_parseProfileConfigs(t *testing.T) {
	saveLockEnv(t)

	tcs := []struct {
		in            string
		key           ProfileName
		expect        []string
		expectError   error
		expectMissing bool
		n             int
	}{
		{
			in:     "foo",
			key:    "foo",
			expect: nil,
			n:      1,
		},
		{
			in:  "foo=bar",
			key: "foo",
			expect: []string{
				"bar",
			},
			n: 1,
		},
		{
			in:  "first=one=1",
			key: "first",
			expect: []string{
				"one=1",
			},
			n: 1,
		},
		{
			in:  "foo=bar:first=one=1",
			key: "first",
			expect: []string{
				"one=1",
			},
			n: 2,
		},
		{
			in:  "foo=bar:first=one=1,two=2",
			key: "first",
			expect: []string{
				"one=1",
				"two=2",
			},
			n: 2,
		},
		{
			in:  "foo=bar:first=one=1,two=2:second:third",
			key: "first",
			expect: []string{
				"one=1",
				"two=2",
			},
			n: 4,
		},
		{
			in:  "foo=bar:first=one=1,two=2:second:third",
			key: "foo",
			expect: []string{
				"bar",
			},
			n: 4,
		},
		{
			in:     "foo=bar:first=one=1,two=2:second:third",
			key:    "second",
			expect: nil,
			n:      4,
		},
		{
			in:     "foo=bar:first=one=1,two=2:second:third",
			key:    "third",
			expect: nil,
			n:      4,
		},
		{
			in:            "=",
			key:           "",
			expectMissing: true,
			expectError:   ErrEmptyProfileName,
		},
		{
			in:            ":",
			key:           "",
			expectMissing: true,
			expectError:   ErrEmptyProfileName,
		},
		{
			in:     ",",
			key:    ",",
			expect: nil,
			n:      1,
		},
		{
			in:            "=,:",
			key:           "",
			expectMissing: true,
			expectError:   ErrEmptyProfileName,
		},
		{
			in:            "",
			key:           "",
			expectMissing: true,
			expectError:   ErrEmptyProfileName,
		},
		{
			in:            ":=",
			key:           "cpu",
			expectMissing: true,
			expectError:   ErrEmptyProfileName,
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d %s", i, tc.in), func(t *testing.T) {
			pbs, err := parseProfileConfigs(1<<10, tc.in)
			require.ErrorIs(t, tc.expectError, err)
			require.Len(t, pbs, tc.n)
			pb, ok := pbs[tc.key] // no negative testing for missing keys (see newProfileConfigs)
			require.Equalf(t, !tc.expectMissing, ok, "key %q for set %q expect missing %t", tc.key, maps.Keys(pbs), tc.expectMissing)
			if tc.expectMissing {
				return
			}
			require.Equal(t, 1<<10, pb.buf.Cap()) // bufsize is always 1024
			require.Equal(t, 0, pb.buf.Len())
			require.Equal(t, tc.expect, pb.flags)
		})
	}
}

func TestDebug_newProfileConfigs(t *testing.T) {
	saveLockEnv(t)

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
			require.Equal(t, 1<<10, pb.buf.Cap()) // bufsize is always 1024
			v, ok := pb.GetValue(tc.key)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.expect, v)
		})
	}
}

func TestDebug_DumpPem(t *testing.T) {
	saveLockEnv(t)

	tcs := []struct {
		inBody      string
		inType      string
		inError     error
		inMx        int
		expectErr   error
		expectLines int
		expectCount int
	}{
		{
			inBody:      "this is a sample PEM",
			inType:      "cpu",
			inMx:        100,
			expectErr:   io.EOF,
			expectLines: 4,
			expectCount: 7,
		},
		{
			inBody:      "this is a sample PEM",
			inType:      "cpu",
			inError:     nil,
			inMx:        5,
			expectErr:   io.EOF,
			expectLines: 0,
			expectCount: 0,
		},
		{
			inBody:      "this is a sample PEM",
			inType:      "cpu",
			inError:     io.EOF,
			inMx:        5,
			expectErr:   io.EOF,
			expectLines: 0,
			expectCount: 0,
		},
		{
			inBody:      "this is a sample PEM",
			inType:      "cpu",
			inError:     io.ErrClosedPipe,
			inMx:        5,
			expectErr:   io.ErrClosedPipe,
			expectLines: 0,
			expectCount: 0,
		},
		{
			inBody:      "this is a sample PEM",
			inType:      "cpu",
			inError:     io.ErrClosedPipe,
			inMx:        5,
			expectErr:   io.ErrClosedPipe,
			expectLines: 0,
			expectCount: 0,
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d %s", i, tc.inType), func(t *testing.T) {
			ctx := context.Background()
			// PEM headings always in upper case
			unm := strings.ToUpper(tc.inType)
			// DumpPem dump a PEM version of the byte slice, bs, into writer, wrt.
			wrt := &ErrorWriter{bs: make([]byte, 0), mx: tc.inMx, err: tc.inError}
			err := DumpPem(ctx, []byte(tc.inBody), unm, wrt)
			if tc.expectErr != nil {
				require.ErrorIs(t, err, tc.expectErr)
			} else {
				require.NoError(t, err)
			}
			dumps := string(wrt.bs)
			j := 0
			rdr := bufio.NewReader(strings.NewReader(dumps))
			for {
				_, err = rdr.ReadBytes('\n')
				if err != nil {
					break
				}
				j++
			}
			require.ErrorIs(t, err, io.EOF)
			require.Equal(t, tc.expectLines, j)
			ssm := pemRegexp.FindAllStringSubmatch(dumps, 100)
			if tc.expectCount > 0 {
				require.Len(t, ssm, 1)
				require.Len(t, ssm[0], tc.expectCount)
			}
		})
	}
}

func TestDebug_LoadProfileConfigs(t *testing.T) {
	// save environment and restore after testing
	saveLockEnv(t)

	ctx := context.Background()

	tcs := []struct {
		inArgs                       string
		profileKey                   ProfileName
		profileFlagKey               string
		expectProfileFlagValue       string
		expectProfileFlagExists      bool
		expectConfigurationCount     int
		expectError                  error
		expectProfileConfigNotExists bool
	}{
		{
			inArgs:                       "",
			expectConfigurationCount:     0,
			profileKey:                   "",
			expectError:                  nil,
			expectProfileConfigNotExists: true,
		},
		{
			inArgs:                   "block=rate=10:cpu:mutex=10",
			expectConfigurationCount: 3,
			profileKey:               "block",
			profileFlagKey:           "rate",
			expectProfileFlagExists:  true,
			expectProfileFlagValue:   "10",
			expectError:              nil,
		},
		{
			inArgs:                   "block=rate=10:cpu:mutex=10",
			expectConfigurationCount: 3,
			profileKey:               "cpu",
			profileFlagKey:           "rate",
			expectProfileFlagExists:  false,
		},
		{
			inArgs:                   "block=rate=10:cpu:mutex=10",
			expectConfigurationCount: 3,
			profileKey:               "mutex",
			profileFlagKey:           "10",
			expectProfileFlagExists:  true,
		},
		{
			inArgs:                       "mutex=10",
			expectConfigurationCount:     1,
			profileKey:                   "cpu",
			profileFlagKey:               "10",
			expectProfileConfigNotExists: true,
		},
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d: %q", i, tc.inArgs), func(t *testing.T) {
			pmp, err := LoadProfileConfig(ctx, tc.inArgs)
			require.ErrorIs(t, tc.expectError, err)
			if err != nil {
				return
			}
			val, ok := pmp[tc.profileKey]
			require.Equalf(t, tc.expectProfileConfigNotExists, !ok, "expecting key %q to %t exist", tc.profileKey, !tc.expectProfileConfigNotExists)
			if tc.expectProfileConfigNotExists {
				return
			}
			flagValue, ok := val.GetValue(tc.profileFlagKey)
			require.Equal(t, tc.expectProfileFlagExists, ok, "expecting key %q to %t exist", tc.profileKey, tc.expectProfileFlagExists)
			if tc.expectProfileFlagExists {
				return
			}
			require.Equal(t, tc.expectProfileFlagValue, flagValue)
		})
	}
}

//nolint:gocritic
func saveLockEnv(t *testing.T) {
	t.Helper()

	oldEnv := os.Getenv(EnvVarKopiaDebugPprof)

	t.Cleanup(func() {
		// restore the old environment
		t.Setenv(EnvVarKopiaDebugPprof, oldEnv)
	})
}

func TestErrorWriter(t *testing.T) {
	eww := &ErrorWriter{mx: 5, err: io.EOF}
	n, err := eww.WriteString("Hello World")
	require.ErrorIs(t, io.EOF, err)
	require.Equal(t, 5, n)
	require.Equal(t, "Hello", string(eww.bs))
}

// ErrorWriter allows injection of errors into the write stream.  There are a few
// failures in PPROF dumps that are worth modeling for tests ([io.EOF] is one)
// For use specify the error, ErrorWriter.err, and byte index, ErrorWriter.mx,
// in which it should occur.
type ErrorWriter struct {
	bs  []byte
	mx  int
	err error
}

func (p *ErrorWriter) Write(bs []byte) (int, error) {
	n := len(bs)

	if len(bs)+len(p.bs) > p.mx {
		// error will be produced at p.mx
		// so don't return any more than
		// n
		n = p.mx - len(p.bs)
	}

	// append the bytes to the local buffer just
	// in case someone wants to know.
	p.bs = append(p.bs, bs[:n]...)
	if n < len(bs) {
		// here we assume that any less than len(bs)
		// bytes written returns an error.  This
		// allows setting ErrorWriter up once
		// to produce an error after multiple
		// writes
		return n, p.err
	}

	return n, nil
}

//nolint:gocritic
func (p *ErrorWriter) WriteString(s string) (int, error) {
	return p.Write([]byte(s))
}
