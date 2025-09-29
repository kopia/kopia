package cli

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"
)

type profileFlags struct {
	profileGCBeforeSaving bool
	profileCPU            bool
	profileBlockingRate   int
	profileMemoryRate     int
	profileMutexFraction  int
	saveProfiles          bool

	outputDirectory  string
	cpuProfileCloser func()
}

func (c *profileFlags) setup(app *kingpin.Application) {
	c.profileBlockingRate = -1
	c.profileMemoryRate = -1
	c.profileMutexFraction = -1

	app.Flag("profile-store-on-exit", "Writes profiling data on exit. It writes a file per profile type (heap, goroutine, threadcreate, block, mutex) in a sub-directory in the directory specified with the --diagnostics-output-directory").Hidden().BoolVar(&c.saveProfiles) //nolint:lll
	app.Flag("profile-go-gc-before-dump", "Perform a Go GC before writing out memory profiles").Hidden().BoolVar(&c.profileGCBeforeSaving)
	app.Flag("profile-blocking-rate", "Blocking profiling rate, a value of 0 turns off block profiling").Hidden().IntVar(&c.profileBlockingRate)
	app.Flag("profile-cpu", "Enable CPU profiling").Hidden().BoolVar(&c.profileCPU)
	app.Flag("profile-memory-rate", "Memory profiling rate").Hidden().IntVar(&c.profileMemoryRate)
	app.Flag("profile-mutex-fraction", "Mutex profiling, a value of 0 turns off mutex profiling").Hidden().IntVar(&c.profileMutexFraction)
}

func (c *profileFlags) start(ctx context.Context, outputDirectory string) error {
	pBlockingRate := c.profileBlockingRate
	pMemoryRate := c.profileMemoryRate
	pMutexFraction := c.profileMutexFraction

	if c.saveProfiles {
		// when saving profiles ensure profiling parameters have sensible values
		// unless explicitly modified.
		// runtime.MemProfileRate has a default value, no need to reset it.
		if pBlockingRate == -1 {
			pBlockingRate = 1
		}

		if pMutexFraction == -1 {
			pMutexFraction = 1
		}
	}

	// set profiling parameters if they have been changed from defaults
	if pBlockingRate != -1 {
		runtime.SetBlockProfileRate(pBlockingRate)
	}

	if pMemoryRate != -1 {
		runtime.MemProfileRate = pMemoryRate
	}

	if pMutexFraction != -1 {
		runtime.SetMutexProfileFraction(pMutexFraction)
	}

	if !c.profileCPU && !c.saveProfiles {
		return nil
	}

	// ensure upfront that the pprof output dir can be created.
	pprofDir, err := mkSubdirectories(outputDirectory, "profiles")
	if err != nil {
		return err
	}

	c.outputDirectory = pprofDir

	if !c.profileCPU {
		return nil
	}

	// start CPU profile dumper
	f, err := os.Create(filepath.Join(pprofDir, "cpu.pprof")) //nolint:gosec
	if err != nil {
		return errors.Wrap(err, "could not create CPU profile output file")
	}

	// CPU profile closer
	closer := func() {
		pprof.StopCPUProfile()

		if err := f.Close(); err != nil {
			log(ctx).Warn("error closing CPU profile output file:", err)
		}
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		closer()

		return errors.Wrap(err, "could not start CPU profile")
	}

	c.cpuProfileCloser = closer

	return nil
}

func (c *profileFlags) stop(ctx context.Context) {
	if c.cpuProfileCloser != nil {
		c.cpuProfileCloser()
		c.cpuProfileCloser = nil
	}

	if !c.saveProfiles {
		return
	}

	if c.profileGCBeforeSaving {
		// update profiles, otherwise they may not include activity after the last GC
		runtime.GC()
	}

	pprofDir, err := mkSubdirectories(c.outputDirectory)
	if err != nil {
		log(ctx).Warn("cannot create directory to save profiles:", err)
	}

	for _, p := range pprof.Profiles() {
		func() {
			fname := filepath.Join(pprofDir, p.Name()+".pprof")

			f, err := os.Create(fname) //nolint:gosec
			if err != nil {
				log(ctx).Warnf("unable to create profile output file '%s': %v", fname, err)

				return
			}

			defer func() {
				if err := f.Close(); err != nil {
					log(ctx).Warnf("unable to close profile output file '%s': %v", fname, err)
				}
			}()

			if err := p.WriteTo(f, 0); err != nil {
				log(ctx).Warnf("unable to write profile to file '%s': %v", fname, err)
			}
		}()
	}
}
