// Package pproflogging for pproflogging helper functions.
package pproflogging

import (
	"bufio"
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("kopia/pproflogging")

// ProfileName the name of the profile (see: runtime/pprof/Lookup).
type ProfileName string

const (
	pair = 2
	// PPROFDumpTimeout when dumping PPROF data, set an upper bound on the time it can take to log.
	PPROFDumpTimeout = 15 * time.Second
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

// flags used to configure profiling in EnvVarKopiaDebugPprof.
const (
	// KopiaDebugFlagForceGc force garbage collection before dumping heap data.
	KopiaDebugFlagForceGc = "forcegc"
	// KopiaDebugFlagDebug value of the profiles `debug` parameter.
	KopiaDebugFlagDebug = "debug"
	// KopiaDebugFlagRate rate setting for the named profile (if available). always an integer.
	KopiaDebugFlagRate = "rate"
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
	// ErrEmptyProfileName returned when a profile configuration flag has no argument.
	ErrEmptyProfileName = errors.New("empty profile flag")

	//nolint:gochecknoglobals
	pprofConfigs = newProfileConfigs(os.Stderr)
)

// Writer interface supports destination for PEM output.
type Writer interface {
	io.Writer
	io.StringWriter
}

// ProfileConfigs configuration flags for all requested profiles.
type ProfileConfigs struct {
	mu sync.Mutex
	// wrt represents the final destination for the PPROF PEM output.  Typically,
	// this is attached to stderr or log output.  A custom writer is used because
	// not all loggers support line oriented output through the io.Writer interface...
	// support is often attached th a io.StringWriter.
	// +checklocks:mu
	wrt Writer
	// +checklocks:mu
	pcm map[ProfileName]*ProfileConfig
}

type pprofSetRate struct {
	setter       func(int)
	defaultValue int
}

//nolint:gochecknoglobals
var pprofProfileRates = map[ProfileName]pprofSetRate{
	ProfileNameBlock: {
		setter:       func(x int) { runtime.SetBlockProfileRate(x) },
		defaultValue: DefaultDebugProfileRate,
	},
	ProfileNameMutex: {
		setter:       func(x int) { runtime.SetMutexProfileFraction(x) },
		defaultValue: DefaultDebugProfileRate,
	},
}

func newProfileConfigs(wrt Writer) *ProfileConfigs {
	q := &ProfileConfigs{
		wrt: wrt,
	}

	return q
}

// LoadProfileConfig configure PPROF profiling from the config in ppconfigss.
func LoadProfileConfig(ctx context.Context, ppconfigss string) (map[ProfileName]*ProfileConfig, error) {
	// if empty, then don't bother configuring but emit a log message - user might be expecting them to be configured
	if ppconfigss == "" {
		log(ctx).Debug("no profile configuration. skipping PPROF setup")
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
func (p ProfileConfig) GetValue(s string) (string, bool) {
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

		if len(profileFlagNameValuePairs) > 1 {
			// only <key>=<value> allowed
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

func setupProfileFractions(ctx context.Context, profileBuffers map[ProfileName]*ProfileConfig) {
	for k, pprofset := range pprofProfileRates {
		v, ok := profileBuffers[k]
		if !ok {
			// profile not configured - leave it alone
			continue
		}

		if v == nil {
			// profile configured, but no rate - set to default
			pprofset.setter(pprofset.defaultValue)
			continue
		}

		s, _ := v.GetValue(KopiaDebugFlagRate)
		if s == "" {
			// flag without an argument - set to default
			pprofset.setter(pprofset.defaultValue)
			continue
		}

		n1, err := strconv.Atoi(s)
		if err != nil {
			log(ctx).With("cause", err).Warnf("invalid PPROF rate, %q, for '%s'", s, k)
			continue
		}

		log(ctx).Debugf("setting PPROF rate, %d, for %s", n1, k)
		pprofset.setter(n1)
	}
}

// clearProfileFractions set the profile fractions to their zero values.
func clearProfileFractions(profileBuffers map[ProfileName]*ProfileConfig) {
	for k, pprofset := range pprofProfileRates {
		v := profileBuffers[k]
		if v == nil { // fold missing values and empty values
			continue
		}

		_, ok := v.GetValue(KopiaDebugFlagRate)
		if !ok { // only care if a value might have been set before
			continue
		}

		pprofset.setter(0)
	}
}

// StartProfileBuffers start profile buffers for enabled profiles/trace.  Buffers
// are returned in an slice of buffers: CPU, Heap and trace respectively.  class is used to distinguish profiles
// external to kopia.
func StartProfileBuffers(ctx context.Context) {
	ppconfigs := os.Getenv(EnvVarKopiaDebugPprof)
	// if empty, then don't bother configuring but emit a log message - use might be expecting them to be configured
	if ppconfigs == "" {
		log(ctx).Warn("no profile buffers enabled")
		return
	}

	bufSizeB := DefaultDebugProfileDumpBufferSizeB

	// look for matching services.  "*" signals all services for profiling
	log(ctx).Debug("configuring profile buffers")

	// acquire global lock when performing operations with global side-effects
	pprofConfigs.mu.Lock()
	defer pprofConfigs.mu.Unlock()

	var err error

	pprofConfigs.pcm, err = parseProfileConfigs(bufSizeB, ppconfigs)
	if err != nil {
		log(ctx).With("cause", err).Warnf("cannot start PPROF config, %q, due to parse error", ppconfigs)
		return
	}

	// profiling rates need to be set before starting profiling
	setupProfileFractions(ctx, pprofConfigs.pcm)

	// cpu has special initialization
	v, ok := pprofConfigs.pcm[ProfileNameCPU]
	if ok {
		err := pprof.StartCPUProfile(v.buf)
		if err != nil {
			log(ctx).With("cause", err).Warn("cannot start cpu PPROF")
			delete(pprofConfigs.pcm, ProfileNameCPU)
		}
	}
}

// DumpPem dump a PEM version of the byte slice, bs, into writer, wrt.
func DumpPem(bs []byte, types string, wrt *os.File) error {
	// err0 for background process
	var err0 error

	blk := &pem.Block{
		Type:  types,
		Bytes: bs,
	}
	// wrt is likely a line oriented writer, so writing individual lines
	// will make best use of output buffer and help prevent overflows or
	// stalls in the output path.
	pr, pw := io.Pipe()

	// ensure read-end of the pipe is close
	//nolint:errcheck
	defer pr.Close()

	// encode PEM in the background and output in a line oriented
	// fashion - this prevents the need for a large buffer to hold
	// the encoded PEM.
	go func() {
		// writer close on exit of background process
		// pipe writer will not return a meaningful error
		//nolint:errcheck
		defer pw.Close()

		// do the encoding
		err0 = pem.Encode(pw, blk)
	}()

	// connect rdr to pipe reader
	rdr := bufio.NewReader(pr)

	// err1 for reading
	// err2 for writing
	var err1, err2 error
	for err1 == nil && err2 == nil {
		var ln []byte
		ln, err1 = rdr.ReadBytes('\n')
		// err1 can return ln and non-nil err1, so always call write
		_, err2 = wrt.Write(ln)
	}

	// got a write error.  this has precedent
	if err2 != nil {
		return fmt.Errorf("could not write PEM: %w", err2)
	}

	if err0 != nil {
		return fmt.Errorf("could not write PEM: %w", err0)
	}

	if err1 == nil {
		return nil
	}

	// if file does not end in newline, then output one
	if errors.Is(err1, io.EOF) {
		_, err2 = wrt.WriteString("\n")
		if err2 != nil {
			return fmt.Errorf("could not write PEM: %w", err2)
		}

		return io.EOF
	}

	return fmt.Errorf("error reading bytes: %w", err1)
}

func parseDebugNumber(v *ProfileConfig) (int, error) {
	debugs, ok := v.GetValue(KopiaDebugFlagDebug)
	if !ok {
		return 0, nil
	}

	debug, err := strconv.Atoi(debugs)
	if err != nil {
		return 0, fmt.Errorf("could not parse number %q: %w", debugs, err)
	}

	return debug, nil
}

// StopProfileBuffers stop and dump the contents of the buffers to the log as PEMs.  Buffers
// supplied here are from StartProfileBuffers.
func StopProfileBuffers(ctx context.Context) {
	pprofConfigs.mu.Lock()
	defer pprofConfigs.mu.Unlock()

	if pprofConfigs == nil {
		log(ctx).Debug("profile buffers not configured")
		return
	}

	log(ctx).Debug("saving PEM buffers for output")
	// cpu and heap profiles requires special handling
	for k, v := range pprofConfigs.pcm {
		log(ctx).Debugf("stopping PPROF profile %q", k)

		if v == nil {
			continue
		}

		if k == ProfileNameCPU {
			pprof.StopCPUProfile()
			continue
		}

		_, ok := v.GetValue(KopiaDebugFlagForceGc)
		if ok {
			log(ctx).Debug("performing GC before PPROF dump ...")
			runtime.GC()
		}

		debug, err := parseDebugNumber(v)
		if err != nil {
			log(ctx).With("cause", err).Warn("invalid PPROF configuration debug number")
			continue
		}

		pent := pprof.Lookup(string(k))
		if pent == nil {
			log(ctx).Warnf("no system PPROF entry for %q", k)
			delete(pprofConfigs.pcm, k)

			continue
		}

		err = pent.WriteTo(v.buf, debug)
		if err != nil {
			log(ctx).With("cause", err).Warn("error writing PPROF buffer")

			continue
		}
	}
	// dump the profiles out into their respective PEMs
	for k, v := range pprofConfigs.pcm {
		if v == nil {
			continue
		}

		unm := strings.ToUpper(string(k))
		log(ctx).Infof("dumping PEM for %q", unm)

		err := DumpPem(v.buf.Bytes(), unm, os.Stderr)
		if err != nil {
			log(ctx).With("cause", err).Error("cannot write PEM")
		}
	}

	// clear the profile rates and fractions to effectively stop profiling
	clearProfileFractions(pprofConfigs.pcm)
	pprofConfigs.pcm = map[ProfileName]*ProfileConfig{}
}
