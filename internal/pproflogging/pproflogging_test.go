package pproflogging

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
)

var (
	mu     sync.Mutex
	oldEnv string
)

func TestDebug_parseProfileConfigs(t *testing.T) {
	saveLockEnv(t)
	defer restoreUnlockEnv(t)

	tcs := []struct {
		in            string
		key           ProfileName
		expect        []string
		expectError   error
		expectMissing bool
	}{
		{
			in:     "foo",
			key:    "foo",
			expect: nil,
		},
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
		},
		{
			in:            "=,:",
			key:           "",
			expectMissing: true,
			expectError:   ErrEmptyProfileName,
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d %s", i, tc.in), func(t *testing.T) {
			pbs, err := parseProfileConfigs(1<<10, tc.in)
			require.ErrorIs(t, tc.expectError, err)
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
	defer restoreUnlockEnv(t)

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

func TestDebug_LoadProfileConfigs(t *testing.T) {
	// save environment and restore after testing
	saveLockEnv(t)
	defer restoreUnlockEnv(t)

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

func TestDebug_ProfileBuffersEnabled(t *testing.T) {
	// save environment and restore after testing
	saveLockEnv(t)
	defer restoreUnlockEnv(t)

	ctx := context.Background()

	tcs := []struct {
		options string
		expect  bool
	}{
		{
			// set to empty.  equivalent of no options set.
			"",
			false,
		},
		{
			"cpu",
			true,
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%q", tc.options), func(t *testing.T) {
			t.Setenv(EnvVarKopiaDebugPprof, tc.options)
			pcm, err := LoadProfileConfig(ctx, os.Getenv(EnvVarKopiaDebugPprof))
			require.NoError(t, err)
			pprofConfigs.pcm = pcm
			ok := HasProfileBuffersEnabled()
			require.Equal(t, tc.expect, ok)
		})
	}
}

// +checklocksignore
//
//nolint:gocritic
func saveLockEnv(t *testing.T) {
	t.Helper()

	mu.Lock()
	oldEnv = os.Getenv(EnvVarKopiaDebugPprof)
}

// +checklocksignore
//
//nolint:gocritic
func restoreUnlockEnv(t *testing.T) {
	t.Helper()

	t.Setenv(EnvVarKopiaDebugPprof, oldEnv)
	mu.Unlock()
}
