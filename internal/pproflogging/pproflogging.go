// Package pproflogging for debug helper functions.
package pproflogging

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("kopia/pproflogging")

// ProfileName the name of the profile (see: runtime/pprof/Lookup).
type ProfileName string

const (
	pair = 2
)

const (
	// DefaultDebugProfileRate default sample/data fraction for profile sample collection rates (1/x, where x is the
	// data fraction sample rate).
	DefaultDebugProfileRate = 100
	// DefaultDebugProfileDumpBufferSizeB default size of the pprof output buffer.
	DefaultDebugProfileDumpBufferSizeB = 1 << 17
)

const (
	// EnvVarKopiaDebugPprof environment variable that contains the pprof dump configuration.
	EnvVarKopiaDebugPprof = "KOPIA_PPROF_LOGGING_CONFIG"
)

const (
	// ProfileNameBlock block profile key.
	ProfileNameBlock ProfileName = "block"
	// ProfileNameMutex mutex profile key.
	ProfileNameMutex = "mutex"
	// ProfileNameCPU cpu profile key.
	ProfileNameCPU = "cpu"
)

var (
	// ErrEmptyConfiguration returned when attempt to configure profile buffers without a configuration string.
	ErrEmptyConfiguration = errors.New("empty profile configuration")
	// ErrEmptyProfileName returned when a profile configuration flag has no argument.
	ErrEmptyProfileName = errors.New("empty profile flag")

	//nolint:gochecknoglobals
	pprofConfigs = newProfileConfigs()
)

// ProfileConfigs configuration flags for all requested profiles.
type ProfileConfigs struct {
	mu  sync.Mutex
	pcm map[ProfileName]*ProfileConfig
}

// HasProfileBuffersEnabled return true if pprof profiling is enabled.
func HasProfileBuffersEnabled() bool {
	pprofConfigs.mu.Lock()
	defer pprofConfigs.mu.Unlock()

	return len(pprofConfigs.pcm) != 0
}

func newProfileConfigs() *ProfileConfigs {
	q := &ProfileConfigs{}

	return q
}

// LoadProfileConfig configure PPROF profiling from the config in ppconfigss.
func LoadProfileConfig(ctx context.Context, ppconfigss string) (map[ProfileName]*ProfileConfig, error) {
	// if empty, then don't bother configuring but emit a log message - use might be expecting them to be configured
	if ppconfigss == "" {
		return nil, nil
	}

	bufSizeB := DefaultDebugProfileDumpBufferSizeB

	// look for matching services.  "*" signals all services for profiling
	log(ctx).Info("configuring profile buffers")

	// acquire global lock when performing operations with global side-effects
	return parseProfileConfigs(bufSizeB, ppconfigss)
}

// ProfileConfig configuration flags for a profile.
type ProfileConfig struct {
	flags []string
	buf   *bytes.Buffer
}

// GetValue get the value of the named flag, `s`.  False will be returned
// if the flag does not exist. True will be returned if flag exists without
// a value.
func (p *ProfileConfig) GetValue(s string) (string, bool) {
	if p == nil {
		return "", false
	}

	for _, f := range p.flags {
		kvs := strings.SplitN(f, "=", pair)
		if kvs[0] != s {
			continue
		}

		if len(kvs) == 1 {
			return "", true
		}

		return kvs[1], true
	}

	return "", false
}

func parseProfileConfigs(bufSizeB int, ppconfigs string) (map[ProfileName]*ProfileConfig, error) {
	pbs := map[ProfileName]*ProfileConfig{}
	allProfileOptions := strings.Split(ppconfigs, ":")

	for _, profileOptionWithFlags := range allProfileOptions {
		// of those, see if any have profile specific settings
		profileFlagNameValuePairs := strings.SplitN(profileOptionWithFlags, "=", pair)
		flagValue := ""

		if len(profileFlagNameValuePairs) == 0 {
			return nil, ErrEmptyConfiguration
		} else if len(profileFlagNameValuePairs) > 1 {
			// only <key>=<value? allowed
			flagValue = profileFlagNameValuePairs[1]
		}

		flagKey := ProfileName(profileFlagNameValuePairs[0])
		if flagKey == "" {
			return nil, ErrEmptyProfileName
		}

		pbs[flagKey] = newProfileConfig(bufSizeB, flagValue)
	}

	return pbs, nil
}

// newProfileConfig create a new profiling configuration.
func newProfileConfig(bufSizeB int, ppconfig string) *ProfileConfig {
	q := &ProfileConfig{
		buf: bytes.NewBuffer(make([]byte, 0, bufSizeB)),
	}

	flgs := strings.Split(ppconfig, ",")
	if len(flgs) > 0 && flgs[0] != "" { // len(flgs) > 1 && flgs[0] == "" should never happen
		q.flags = flgs
	}

	return q
}
