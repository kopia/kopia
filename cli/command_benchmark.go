package cli

import (
	"bytes"
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

func runInParallelNoInputNoResult(n int, run func()) {
	dummyArgs := make([]int, n)

	runInParallelNoResult(dummyArgs, func(_ int) {
		run()
	})
}

func runInParallelNoInput[T any](n int, run func() T) T {
	dummyArgs := make([]int, n)

	return runInParallel(dummyArgs, func(_ int) T {
		return run()
	})
}

func runInParallelNoResult[A any](args []A, run func(arg A)) {
	runInParallel(args, func(arg A) any {
		run(arg)
		return nil
	})
}

func runInParallel[A any, T any](args []A, run func(arg A) T) T {
	var wg sync.WaitGroup

	for _, arg := range args[1:] {
		wg.Add(1)

		go func() {
			defer wg.Done()

			run(arg)
		}()
	}

	// run one on the main goroutine and N-1 in parallel.
	v := run(args[0])

	wg.Wait()

	return v
}

func makeOutputBuffers(n, capacity int) []*bytes.Buffer {
	var res []*bytes.Buffer

	for range n {
		res = append(res, bytes.NewBuffer(make([]byte, 0, capacity)))
	}

	return res
}
