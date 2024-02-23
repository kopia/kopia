// Package pproflogging for pprof helper functions.
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

const (
	errMsgCouldNotWritePem = "could not write pem"
)

var (
	// ErrEmptyConfiguration returned when attempt to configure profile buffers without a configuration string.
	ErrEmptyConfiguration = errors.New("empty profile configuration")
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
	//+checklocks:mu
	wrt Writer
	//+checklocks:mu
	pcm map[ProfileName]*ProfileConfig
	src string
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

// HasProfileBuffersEnabled return true if pprof profiling is enabled.
func HasProfileBuffersEnabled() bool {
	pprofConfigs.mu.Lock()
	defer pprofConfigs.mu.Unlock()

	return len(pprofConfigs.pcm) != 0
}

// MaybeStartProfileBuffers start profile buffers for this process with a configuration from the environment.
func MaybeStartProfileBuffers(ctx context.Context) bool {
	return MaybeStartProfileBuffersWithConfig(ctx, os.Getenv(EnvVarKopiaDebugPprof))
}

// MaybeStartProfileBuffersWithConfig start profile buffers for this process with a custom configuration.
func MaybeStartProfileBuffersWithConfig(ctx context.Context, config string) bool {
	pprofConfigs.mu.Lock()
	defer pprofConfigs.mu.Unlock()

	if !loadAndSetProfileBuffersLocked(ctx, config) {
		log(ctx).Debug("no profile buffer configuration to start")
		return false
	}

	pprofConfigs.StartProfileBuffersLocked(ctx)

	return true
}

// MaybeRestartProfileBuffersWithConfig used by SIGQUIT signal handlers to output PPROF data without exiting.
func MaybeRestartProfileBuffersWithConfig(ctx context.Context, config string) bool {
	pprofConfigs.mu.Lock()
	defer pprofConfigs.mu.Unlock()

	if len(pprofConfigs.pcm) == 0 {
		log(ctx).Debug("no profile buffer configuration to restart")
		return false
	}

	pprofConfigs.StopProfileBuffersLocked(ctx)

	if !loadAndSetProfileBuffersLocked(ctx, config) {
		log(ctx).Debug("no profile buffer configuration to start")
		return false
	}

	pprofConfigs.StartProfileBuffersLocked(ctx)

	return true
}

// MaybeStopProfileBuffers stop and dump the contents of the buffers to the log as PEMs.  Buffers
// supplied here are from MaybeStartProfileBuffers.
func MaybeStopProfileBuffers(ctx context.Context) {
	pprofConfigs.mu.Lock()
	defer pprofConfigs.mu.Unlock()

	pprofConfigs.StopProfileBuffersLocked(ctx)
}

func loadAndSetProfileBuffersLocked(ctx context.Context, config string) bool {
	pcm, err := LoadProfileConfig(ctx, config)
	if err != nil {
		log(ctx).With("error", err).Debug("cannot start configured profile buffers")
		return false
	}

	// allow profile dumps to be cleared by setting without prior check
	pprofConfigs.src = config
	pprofConfigs.pcm = pcm

	return len(pprofConfigs.pcm) != 0
}

func newProfileConfigs(wrt Writer) *ProfileConfigs {
	q := &ProfileConfigs{
		wrt: wrt,
	}

	return q
}

// SetWriter set the destination for the PPROF dump.
// +checklocksignore.
func (p *ProfileConfigs) SetWriter(wrt Writer) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.wrt = wrt
}

// GetProfileConfig return a profile configuration by name.
// +checklocksignore.
func (p *ProfileConfigs) GetProfileConfig(nm ProfileName) *ProfileConfig {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.pcm[nm]
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

// StartProfileBuffersLocked start profile buffers for enabled profiles/trace.  Buffers
// are returned in a slice of buffers: CPU, Heap and trace respectively.  class
// is used to distinguish profiles external to kopia.
func (p *ProfileConfigs) StartProfileBuffersLocked(ctx context.Context) {
	// profiling rates need to be set before starting profiling
	setupProfileFractions(ctx, p.pcm)

	// cpu has special initialization
	v, ok := p.pcm[ProfileNameCPU]
	if !ok {
		return
	}

	err := pprof.StartCPUProfile(v.buf)
	if err != nil {
		log(ctx).With("cause", err).Warn("cannot start cpu PPROF")
		delete(p.pcm, ProfileNameCPU)
	}
}

// StopProfileBuffersLocked stop and dump the contents of the buffers to the log as PEMs.  Buffers
// supplied here are from MaybeStartProfileBuffers.
func (p *ProfileConfigs) StopProfileBuffersLocked(ctx context.Context) {
	defer func() {
		// clear the profile rates and fractions to effectively stop profiling
		clearProfileFractions(p.pcm)
		p.pcm = map[ProfileName]*ProfileConfig{}
	}()

	log(ctx).Debugf("saving %d PEM buffers for output", len(p.pcm))

	// cpu and heap profiles requires special handling
	for nm, v := range p.pcm {
		if v == nil {
			// silently ignore empty profiles
			continue
		}

		_, ok := v.GetValue(KopiaDebugFlagForceGc)
		if ok {
			log(ctx).Debug("performing GC before PPROF dump ...")
			runtime.GC()
		}

		// stop CPU profile after GC
		if nm == ProfileNameCPU {
			log(ctx).Debug("stopping CPU profile")

			pprof.StopCPUProfile()

			continue
		}

		// look up the profile.  must not be nil
		pent := pprof.Lookup(string(nm))
		if pent == nil {
			log(ctx).Warnf("no system PPROF entry for %q", nm)
			delete(p.pcm, nm)

			continue
		}

		// parse the debug number if supplied
		debug, err := parseDebugNumber(v)
		if err != nil {
			log(ctx).With("cause", err).Warnf("%q: invalid PPROF configuration debug number", nm)
			continue
		}

		// write the profile data to the buffer associated with the profile
		err = pent.WriteTo(v.buf, debug)
		if err != nil {
			log(ctx).With("cause", err).Warnf("%q: error writing PPROF buffer", nm)
			continue
		}

		// check context for break
		if ctx.Err() != nil {
			log(ctx).With("cause", err).Warnf("%q: error writing PPROF buffer", nm)
			break
		}
	}

	// dump the profiles out into their respective PEMs
	for k, v := range p.pcm {
		if v == nil {
			// no profile config, loop
			continue
		}

		// PEM headings always in upper case
		unm := strings.ToUpper(string(k))

		log(ctx).Debugf("dumping PEM for %q", unm)

		err := DumpPem(ctx, v.buf.Bytes(), unm, p.wrt)
		if err != nil {
			log(ctx).With("cause", err).Errorf("%q: cannot write PEM", unm)
			return
		}

		// process context
		err = ctx.Err()
		if err != nil {
			// ctx context may be bad, so use context.Background for safety
			log(ctx).With("cause", err).Warnf("%q: cannot write PEM", unm)
			return
		}
	}
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

		if len(profileFlagNameValuePairs) > 1 {
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

// setupProfileFractions somewhat complex setup for profile buffers.  The intent
// is to implement a generic method for setting up _any_ pprofule.  This is done
// in anticipation of using different or custom profiles.
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
			log(ctx).With("cause", err).Warnf("invalid PPROF rate, %q, for %s: %v", s, k)
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
		log(ctx).Debug("no profile buffers enabled")
		return
	}

	bufSizeB := DefaultDebugProfileDumpBufferSizeB

	// look for matching services.  "*" signals all services for profiling
	log(ctx).Debug("configuring profile buffers")

	// acquire global lock when performing operations with global side-effects
	pprofConfigs.mu.Lock()
	defer pprofConfigs.mu.Unlock()

	pprofConfigs.pcm, _ = parseProfileConfigs(bufSizeB, ppconfigs)

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
func DumpPem(ctx context.Context, bs []byte, types string, wrt Writer) error {
	blk := &pem.Block{
		Type:  types,
		Bytes: bs,
	}

	// wrt is likely a line oriented writer, so writing individual lines
	// will make best use of output buffer and help prevent overflows or
	// stalls in the output path.
	pr, pw := io.Pipe()

	// encode PEM in the background and output in a line oriented
	// fashion - this prevents the need for a large buffer to hold
	// the encoded PEM.
	go func() {
		// do the encoding
		err0 := pem.Encode(pw, blk)
		if err0 != nil {
			// got a write error.
			log(ctx).With("cause", err0).Error(errMsgCouldNotWritePem)
		}

		// writer close on exit of background process
		// pipe writer will not return a meaningful error
		//nolint:errcheck
		pw.Close()
	}()

	// connect rdr to pipe reader
	rdr := bufio.NewReader(pr)

	// err1 for reading
	// err2 for writing
	// err3 for context
	var err1, err2, err3 error
	err3 = ctx.Err()

	// A missed line or partial line in console output will result in a corrupted PEM.
	// Because of this, it is important that output conform to external loggers,
	// especially Kubernetes.
	// this code ensures that output occurs in a line oriented way so that there are
	// no broken lines or missing terminal lines.
	//
	// output as long as there are no:
	// - errors writing to the log
	// - reading from the pem encodings
	// - canceled contexts
	//
	for err1 == nil && err2 == nil && err3 == nil {
		var ln []byte
		// ReadBytes may hang and ignore context timout
		// err1 can return ln and non-nil err1, so always call write
		ln, err1 = rdr.ReadBytes('\n')
		// Write may hang and ignore context timout
		// write line to output
		_, err2 = wrt.Write(ln)
		// check context timeout between the two.  If a stall occurs on
		// any of the IO operations above, it will rely on the OS/golang
		// library timeout to return
		// (golang really needs IO functions with better support for
		// timeouts)
		err3 = ctx.Err()
	}

	// cleanup the reader end of the pipe.
	// pipe writer will not return a meaningful error
	//nolint:errcheck
	pr.Close()

	if err3 != nil {
		// got a context error.  this has precedent
		// err2 and err1 may be non-nil but err3 is the "cause"
		// so report that.
		return fmt.Errorf("%s: %w", errMsgCouldNotWritePem, err3)
	}

	if err2 != nil {
		// got a write error.
		return fmt.Errorf("%s: %w", errMsgCouldNotWritePem, err2)
	}

	// lines are flushed one at a time.  Do not risk omitting a
	// newline at the end of a dump
	_, err2 = wrt.WriteString("\n")
	if err2 != nil {
		return fmt.Errorf("%s: %w", errMsgCouldNotWritePem, err2)
	}

	// did not get a read error.  file ends in newline
	if err1 != nil && !errors.Is(err1, io.EOF) {
		return fmt.Errorf("error reading bytes: %w", err1)
	}

	return nil
}

// parseDebugNumber debug numbers are supported by Go PPROF logging
// the interpretation of debug numbers is left up to the profile
// implementation.  This library is permissive on the debug number
// values, so it makes no attempt to determine what's valid.
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

		err := DumpPem(ctx, v.buf.Bytes(), unm, os.Stderr)
		if err != nil {
			log(ctx).With("cause", err).Error("cannot write PEM")
		}
	}

	// clear the profile rates and fractions to effectively stop profiling
	clearProfileFractions(pprofConfigs.pcm)
	pprofConfigs.pcm = map[ProfileName]*ProfileConfig{}
}
