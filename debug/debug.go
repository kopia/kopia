package debug

import (
	"bufio"
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"

	errors2 "gopkg.in/errgo.v2/fmt/errors"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("kopia/debug")

// ProfileName the name of the profile (see: runtime/pprof/Lookup)
type ProfileName string

const (
	// DefaultDebugProfileRate default sample/data fraction for profile sample collection rates (1/x, where x is the
	// data fraction sample rate).
	DefaultDebugProfileRate = 100
	// DefaultDebugProfileDumpBufferSizeB default size of the pprof output buffer.
	DefaultDebugProfileDumpBufferSizeB = 1 << 17
)

const (
	// EnvVarKopiaDebugPprof environment variable that contains the pprof dump configuration.
	EnvVarKopiaDebugPprof = "KOPIA_DEBUG_PPROF"
)

// flags used to configure profiling in EnvVarKopiaDebugPprof
const (
	// KopiaDebugFlagForceGc force garbage collection before dumping heap data.
	KopiaDebugFlagForceGc = "forcegc"
	// KopiaDebugFlagDebug value of the profiles `debug` parameter.
	KopiaDebugFlagDebug = "debug"
	// KopiaDebugFlagRate rate setting for the named profile (if available). always an integer.
	KopiaDebugFlagRate = "rate"
)

const (
	ProfileNameBlock ProfileName = "block"
	ProfileNameMutex             = "mutex"
	ProfileNameCpu               = "cpu"
)

// ProfileConfig configuration flags for a profile.
type ProfileConfig struct {
	flags []string
	buf   *bytes.Buffer
}

// ProfileConfigs configuration flags for all requested profiles.
type ProfileConfigs struct {
	mu  sync.Mutex
	pcm map[ProfileName]*ProfileConfig
}

var (
	pprofConfigs = &ProfileConfigs{}
)

type pprofSetRate struct {
	fn  func(int)
	def int
}

var pprofProfileRates = map[ProfileName]pprofSetRate{
	ProfileNameBlock: {
		fn:  func(x int) { runtime.SetBlockProfileRate(x) },
		def: DefaultDebugProfileRate,
	},
	ProfileNameMutex: {
		fn:  func(x int) { runtime.SetMutexProfileFraction(x) },
		def: DefaultDebugProfileRate,
	},
}

// GetValue get the value of the named flag, `s`.  false will be returned
// if the flag value does not exist
func (p ProfileConfig) GetValue(s string) (string, bool) {
	for _, f := range p.flags {
		kvs := strings.SplitN(f, "=", 2)
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

func parseProfileConfigs(bufSizeB int, ppconfigs string) map[ProfileName]*ProfileConfig {
	pbs := map[ProfileName]*ProfileConfig{}
	allProfileOptions := strings.Split(ppconfigs, ":")
	for _, profileOptionWithFlags := range allProfileOptions {
		// of those, see if any have profile-specific settings
		profileFlagNameValuePairs := strings.SplitN(profileOptionWithFlags, "=", 2)
		flagValue := ""
		if len(profileFlagNameValuePairs) > 1 {
			flagValue = profileFlagNameValuePairs[1]
		}
		flagKey := ProfileName(strings.ToLower(profileFlagNameValuePairs[0]))
		pbs[flagKey] = newProfileConfig(bufSizeB, flagValue)
	}
	return pbs
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
			pprofset.fn(pprofset.def)
			continue
		}
		s, _ := v.GetValue(KopiaDebugFlagRate)
		if s == "" {
			// flag without an argument - set to default
			pprofset.fn(pprofset.def)
			continue
		}
		n1, err := strconv.Atoi(s)
		if err != nil {
			log(ctx).With("cause", err).Warnf("invalid PPROF rate, %q, for %s: %v", s, k)
			continue
		}
		log(ctx).Debugf("setting PPROF rate, %d, for %s", n1, k)
		pprofset.fn(n1)
	}
}

// ClearProfileFractions set the profile fractions to their zero values.
func ClearProfileFractions(profileBuffers map[ProfileName]*ProfileConfig) {
	for k, pprofset := range pprofProfileRates {
		v := profileBuffers[k]
		if v == nil { // fold missing values and empty values
			continue
		}
		_, ok := v.GetValue(KopiaDebugFlagRate)
		if !ok { // only care if a value might have been set before
			continue
		}
		pprofset.fn(0)
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

	// aquire global lock when performing operations with global side-effects
	pprofConfigs.mu.Lock()
	defer pprofConfigs.mu.Unlock()

	pprofConfigs.pcm = parseProfileConfigs(bufSizeB, ppconfigs)

	// profiling rates need to be set before starting profiling
	setupProfileFractions(ctx, pprofConfigs.pcm)

	// cpu has special initialization
	v, ok := pprofConfigs.pcm[ProfileNameCpu]
	if ok {
		err := pprof.StartCPUProfile(v.buf)
		if err != nil {
			log(ctx).With("cause", err).Warn("cannot start cpu PPROF")
			delete(pprofConfigs.pcm, ProfileNameCpu)
		}
	}
}

// DumpPem dump a PEM version of the byte slice, bs, into writer, wrt.
func DumpPem(bs []byte, types string, wrt *os.File) error {
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
		defer pw.Close()
		err := pem.Encode(pw, blk)
		if err != nil {
			return
		}
	}()
	rdr := bufio.NewReader(pr)
	var err0, err1 error
	for err0 == nil && err1 == nil {
		var ln []byte
		ln, err0 = rdr.ReadBytes('\n')
		_, err1 = wrt.Write(ln)
	}
	// got a write error
	if err1 != nil {
		return err1
	}
	// did not get a read error.  file ends in newline
	if err0 == nil {
		return nil
	}
	// if file does not end in newline, then output one
	if errors.Is(err0, io.EOF) {
		_, err1 = wrt.WriteString("\n")
		// TODO: lint says errors need to be wrapped ... figure out how its dealth with in rest of kopia
		return errors2.Wrap(err0)
	}
	return err0
}

func parseDebugNumber(v *ProfileConfig) (int, error) {
	debugs, ok := v.GetValue(KopiaDebugFlagDebug)
	if !ok {
		return 0, nil
	}
	debug, err := strconv.Atoi(debugs)
	if err != nil {
		return 0, err
	}
	return debug, nil
}

// StopProfileBuffers stop and dump the contents of the buffers to the log as PEMs.  Buffers
// supplied here are from StartProfileBuffers
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
		if k == ProfileNameCpu {
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
	ClearProfileFractions(pprofConfigs.pcm)
	pprofConfigs.pcm = map[ProfileName]*ProfileConfig{}
}
