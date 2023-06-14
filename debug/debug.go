package debug

import (
	"bufio"
	"bytes"
	"context"
	"encoding/pem"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"sync"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("kopia/debug")

type ProfileName string

const (
	DefaultDebugProfileDumpBufferSizeB = 1 << 17
)

const (
	EnvVarKopiaDebugPprof = "KOPIA_DEBUG_PPROF"
	KopiaDebugFlagForceGc = "forcegc"
	KopiaDebugFlagDebug   = "debug"
)

const (
	ProfileNameGoroutine    ProfileName = "goroutine"
	ProfileNameThreadcreate             = "threadcreate"
	ProfileNameHeap                     = "heap"
	ProfileNameAllocs                   = "allocs"
	ProfileNameBlock                    = "block"
	ProfileNameMutex                    = "mutex"
	ProfileNameTrace                    = "trace"
	ProfileNameCpu                      = "cpu"
)

type ProfileConfig struct {
	flags []string
	buf   *bytes.Buffer
}

var (
	pprofMu             = sync.Mutex{}
	pprofProfileConfigs = map[ProfileName]*ProfileConfig{}
)

var pprofProfileRates = map[ProfileName]func(int){
	ProfileNameBlock: func(x int) {
		runtime.SetBlockProfileRate(x)
	},
	ProfileNameMutex: func(x int) {
		runtime.SetMutexProfileFraction(x)
	},
}

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
	allProfileOptions := strings.Split(ppconfigs, ",")
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

func newProfileConfig(bufSizeB int, ppconfig string) *ProfileConfig {
	q := &ProfileConfig{
		buf: bytes.NewBuffer(make([]byte, 0, bufSizeB)),
	}
	flgs := strings.Split(ppconfig, ":")
	if len(flgs) > 0 && flgs[0] != "" { // len(flgs) > 1 && flgs[0] == "" should never happen
		q.flags = flgs
	}
	return q
}

func setupProfileFractions(ctx context.Context, profileBuffers map[ProfileName]*ProfileConfig) {
	for k, fn := range pprofProfileRates {
		v, ok := profileBuffers[k]
		if !ok {
			continue
		}
		n := 100
		s, _ := v.GetValue("rate")
		if s == "" { // flag without argument is meaningless
			continue
		}
		var err error
		var n1 int
		n1, err = strconv.Atoi(s)
		if err != nil {
			log(ctx).With("cause", err).Warnf("invalid PPROF rate, %q, for %s: %v", s, k)
		} else {
			n = n1
			log(ctx).Debugf("setting PPROF rate, %d, for %s", n, k)
		}
		fn(n)
	}
}

func clearProfileFractions(profileBuffers map[ProfileName]*ProfileConfig) {
	for k, fn := range pprofProfileRates {
		if k == "cpu" {
			continue
		}
		v := profileBuffers[k]
		if v == nil { // fold missing values and empty values
			continue
		}
		_, ok := v.GetValue("rate")
		if !ok { // only care if a value might have been set before
			continue
		}
		fn(0)
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

	// getting to this point means that we had profile configurations but no buffers were setup,
	// which is kinda wierd - so produce a warning and don't do anymore work
	pcfgs := parseProfileConfigs(bufSizeB, ppconfigs)

	pprofMu.Lock()
	defer pprofMu.Unlock()

	// profiling rates need to be set before starting profiling
	setupProfileFractions(ctx, pcfgs)

	// cpu has special initializaion
	v, ok := pcfgs[ProfileNameCpu]
	if ok {
		// signal that the profile buffers have been configured
		err := pprof.StartCPUProfile(v.buf)
		if err != nil {
			log(ctx).With("cause", err).Warn("cannot start cpu PPROF")
			delete(pcfgs, ProfileNameCpu)
		}
	}
	pprofProfileConfigs = pcfgs
}

// DumpPem dump a PEM version of the byte slice, bs, into writer, wrt
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
	for {
		ln, err0 := rdr.ReadBytes('\n')
		_, err1 := wrt.Write(ln)
		if err1 != nil {
			return err1
		}
		if errors.Is(err0, io.EOF) {
			wrt.WriteString("\n")
			break
		}
		if err0 != nil {
			return err0
		}
	}
	return nil
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
	pprofMu.Lock()
	defer pprofMu.Unlock()

	pcfgs := pprofProfileConfigs
	pprofProfileConfigs = map[ProfileName]*ProfileConfig{}

	if pcfgs == nil {
		log(ctx).Debug("profile buffers not configured")
		return
	}

	log(ctx).Debug("saving PEM buffers for output")
	// cpu and heap profiles requires special handling
	for k, v := range pcfgs {
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
			delete(pcfgs, k)
			continue
		}
		err = pent.WriteTo(v.buf, debug)
		if err != nil {
			log(ctx).With("cause", err).Warn("error writing PPROF buffer")
			continue
		}
	}
	// dump the profiles out into their respective PEMs
	for k, v := range pcfgs {
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
	clearProfileFractions(pcfgs)
}
