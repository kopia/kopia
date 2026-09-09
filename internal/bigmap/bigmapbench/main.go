// Command bigmapbench provides a benchmark for the bigmap implementation.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/kopia/kopia/internal/bigmap"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/logging"
)

const (
	implSyncMap           = 0
	implMapWithEmptyValue = 1
	implMapWithValues     = 2
)

//nolint:gochecknoglobals
var (
	impl              = flag.Int("impl", implMapWithEmptyValue, "Select implementation")
	profileDir        = flag.String("profile-dir", "", "Profile directory")
	profileCPU        = flag.Bool("profile-cpu", false, "Profile CPU")
	profileMemory     = flag.Bool("profile-memory", false, "Profile memory usage")
	profileMemoryRate = flag.Int("profile-memory-rate", -1, "Profile memory rate")
)

func main() {
	flag.Parse()

	ctx := logging.WithLogger(context.Background(), logging.ToWriter(os.Stderr))

	var (
		bm     *bigmap.Map
		sm     *sync.Map
		num    [8]byte
		keyBuf [sha256.Size]byte
		ms0    runtime.MemStats
	)

	defer maybeStartProfiling().stop()

	switch *impl {
	case implSyncMap:
		sm = &sync.Map{}

		fmt.Println("using sync.Map")
	case implMapWithEmptyValue:
		fmt.Println("using bigmap.Map without values")

		bm, _ = bigmap.NewMapWithOptions(ctx, &bigmap.Options{})
	case implMapWithValues:
		fmt.Println("using bigmap.Map with values")

		bm, _ = bigmap.NewMapWithOptions(ctx, &bigmap.Options{})
	}

	h := sha256.New()

	runtime.ReadMemStats(&ms0)

	t0 := clock.Now()

	for i := range 300_000_000 {
		// generate key=sha256(i) without allocations.
		h.Reset()
		binary.LittleEndian.PutUint64(num[:], uint64(i))
		h.Write(num[:])
		h.Sum(keyBuf[:0])

		switch *impl {
		case implSyncMap:
			sm.LoadOrStore(keyBuf, nil)
		case implMapWithEmptyValue:
			bm.PutIfAbsent(ctx, keyBuf[:], nil)
		case implMapWithValues:
			bm.PutIfAbsent(ctx, keyBuf[:], keyBuf[:])
		}

		count := uint64(i + 1)

		if count%1_000_000 == 0 {
			var ms runtime.MemStats

			runtime.ReadMemStats(&ms)

			alloc := ms.HeapAlloc - ms0.HeapAlloc
			dur := clock.Now().Sub(t0)

			fmt.Printf("elapsed %v, count: %v M, bytes: %v MB, bytes/item: %v, Mitems/sec: %.1f\n",
				dur.Truncate(time.Second),
				float64(count)/1e6,
				alloc/1e6,
				alloc/count,
				float64(count)/dur.Seconds()/1e6)
		}
	}
}

// dirMode is the directory mode for output directories.
const dirMode = 0o700

type stopperFn func()

func (f stopperFn) stop() {
	f()
}

func maybeStartProfiling() stopperFn {
	if *profileDir == "" {
		return func() {}
	}

	// ensure upfront that the pprof output dir can be created.
	if err := os.MkdirAll(*profileDir, dirMode); err != nil {
		log.Fatalln("could not create directory for profile output:", err)
	}

	var cpuProfileStopper stopperFn

	if *profileCPU {
		cpuProfileStopper = startCPUProfiling(*profileDir)
	}

	if *profileMemory && *profileMemoryRate >= 0 {
		runtime.MemProfileRate = *profileMemoryRate
	}

	return func() {
		if cpuProfileStopper != nil {
			cpuProfileStopper()
		}

		if *profileMemory {
			dumpProfiles(*profileDir)
		}
	}
}

func startCPUProfiling(profDir string) stopperFn {
	// start CPU profile dumper
	f, err := os.Create(filepath.Join(profDir, "cpu.pprof")) //nolint:gosec
	if err != nil {
		log.Fatalln("could not create CPU profile output file:", err)
	}

	// CPU profile profStopper
	profStopper := func() {
		pprof.StopCPUProfile()

		if err := f.Close(); err != nil {
			log.Println("error closing CPU profile output file:", err)
		}
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		profStopper()

		log.Fatalln("could not start CPU profile:", err)
	}

	return profStopper
}

func dumpProfiles(profDir string) {
	if err := os.MkdirAll(profDir, dirMode); err != nil {
		log.Println("could not create directory for profile output:", err)

		return
	}

	runtime.GC() // force GC to include stats since last GC

	for _, p := range pprof.Profiles() {
		func() {
			fname := filepath.Join(profDir, p.Name()+".pprof")

			f, err := os.Create(fname) //nolint:gosec
			if err != nil {
				log.Printf("unable to create profile output file '%s': %v", fname, err)

				return
			}

			defer func() {
				if err := f.Close(); err != nil {
					log.Printf("unable to close profile output file '%s': %v", fname, err)
				}
			}()

			if err := p.WriteTo(f, 0); err != nil {
				log.Printf("unable to write profile to file '%s': %v", fname, err)
			}
		}()
	}
}
