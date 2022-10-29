package cli

import (
	"sync"
)

type commandBenchmark struct {
	compression commandBenchmarkCompression
	crypto      commandBenchmarkCrypto
	hashing     commandBenchmarkHashing
	encryption  commandBenchmarkEncryption
	splitters   commandBenchmarkSplitters
	ecc         commandBenchmarkEcc
}

func (c *commandBenchmark) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("benchmark", "Commands to test performance of algorithms.")

	c.compression.setup(svc, cmd)
	c.crypto.setup(svc, cmd)
	c.splitters.setup(svc, cmd)
	c.hashing.setup(svc, cmd)
	c.encryption.setup(svc, cmd)
	c.ecc.setup(svc, cmd)
}

type cryptoBenchResult struct {
	hash       string
	encryption string
	throughput float64
}

func runInParallelNoResult(parallel int, run func()) {
	runInParallel(parallel, func() any {
		run()
		return nil
	})
}

func runInParallel[T any](parallel int, run func() T) T {
	var wg sync.WaitGroup

	for i := 0; i < parallel-1; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			run()
		}()
	}

	// run one on the main goroutine and N-1 in parallel.
	v := run()

	wg.Wait()

	return v
}
