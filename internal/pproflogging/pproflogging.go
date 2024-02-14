// Package pproflogging for debug helper functions.
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

// flags used to configure profiling in EnvVarKopiaDebugPprof.
const (
	// KopiaDebugFlagDebug value of the profiles `debug` parameter.
	KopiaDebugFlagDebug = "debug"
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
	mu  sync.Mutex
	wrt Writer
	pcm map[ProfileName]*ProfileConfig
}

// HasProfileBuffersEnabled return true if pprof profiling is enabled.
func HasProfileBuffersEnabled() bool {
	pprofConfigs.mu.Lock()
	defer pprofConfigs.mu.Unlock()

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

// DumpPem dump a PEM version of the byte slice, bs, into writer, wrt.
// DumpPem performs the PEM dump in two goroutines: the first (the main
// execution thread) performs the output to the console (see notes below).
// The second gorouting encodes []byte into PEM format.
//
// The advantage of using two goroutines is a simpler implementation
// and better utilization of Go library functions.
// The implementation could be single threaded (unrolling the io.Pipe into
// a single thread), but the utilization of the bufio buffer then becomes
// convoluted (or a custom temporary buffer needs to be allocated).
func DumpPem(ctx context.Context, bs []byte, types string, wrt Writer) error {
	blk := &pem.Block{
		Type:  types,
		Bytes: bs,
	}
	// wrt is likely a line oriented writer, so writing individual lines
	// will make best use of output buffer and help prevent overflows or
	// stalls in the output path.
	pr, pw := io.Pipe()

	// ensure that the reader is closed on exit
	defer func() {
		err := pr.Close()
		if err != nil {
			log(ctx).Errorf("error closing PEM buffer: %w", err)
		}
	}()

	// start a writer in the background.  This writes encoded PEMs while there's data.
	go func() {
		// do the encoding
		err0 := pem.Encode(pw, blk)
		if err0 != nil {
			log(ctx).Errorf("could not write PEM: %w", err0)
		}

		// writer close on exit of background process
		err := pw.Close()
		if err != nil {
			// print error but do not exit
			log(ctx).Errorf("error closing PEM buffer: %w", err)
		}
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
		ln, err1 = rdr.ReadBytes('\n')
		// err1 can return ln and non-nil err1, so always call write
		_, err2 = wrt.Write(ln)
		err3 = ctx.Err()
	}

	// be nice, tell everyone of any problems.

	// got a context error.  this has precedent
	if err3 != nil {
		return fmt.Errorf("could not write PEM: %w", err3)
	}

	// got a write error.
	if err2 != nil {
		return fmt.Errorf("could not write PEM: %w", err2)
	}

	// did not get a read error.  file ends in newline
	if err1 == nil {
		return nil
	}

	// if file does not end in newline, then output one.
	// this prevents truncated output
	if errors.Is(err1, io.EOF) {
		_, err2 = wrt.WriteString("\n")
		if err2 != nil {
			return fmt.Errorf("could not write PEM: %w", err2)
		}

		return nil
	}

	return fmt.Errorf("error reading bytes: %w", err1)
}
