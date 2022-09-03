package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/pkg/profile"

	"github.com/kopia/kopia/internal/bigmap"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/logging"
)

//nolint:gochecknoglobals
var (
	impl       = flag.Int("impl", 0, "Select implementation")
	profileDir = flag.String("profile-dir", "", "Profile directory")
	profileCPU = flag.Bool("profile-cpu", false, "Profile CPU")
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

	if *profileDir != "" {
		pp := profile.ProfilePath(*profileDir)

		if *profileCPU {
			defer profile.Start(pp, profile.CPUProfile).Stop()
		}
	}

	if *impl == 0 {
		sm = &sync.Map{}

		fmt.Println("using sync.Map")
	} else if *impl == 1 {
		fmt.Println("using bigmap.Map")

		bm, _ = bigmap.NewMapWithOptions(ctx, true, &bigmap.Options{})

	}

	h := sha256.New()

	runtime.ReadMemStats(&ms0)

	t0 := clock.Now()

	for i := 0; i < 300_000_000; i++ {
		if i%1_000_000 == 0 && i > 0 {
			var ms runtime.MemStats

			runtime.ReadMemStats(&ms)

			alloc := ms.HeapAlloc - ms0.HeapAlloc
			dur := clock.Now().Sub(t0).Truncate(time.Second)

			fmt.Printf("elapsed %v count: %v M bytes: %v MB bytes/item: %v Mitems/sec: %.1f\n",
				dur,
				float64(i)/1e6,
				alloc/1e6,
				alloc/uint64(i),
				float64(i)/dur.Seconds()/1e6)
		}

		// generate key=sha256(i) without allocations.
		h.Reset()
		binary.LittleEndian.PutUint64(num[:], uint64(i))
		h.Write(num[:])
		h.Sum(keyBuf[:0])

		if *impl == 0 {
			sm.LoadOrStore(keyBuf, nil)
		} else if *impl == 1 {
			bm.PutIfAbsent(ctx, keyBuf[:], nil)
		}
	}
}
