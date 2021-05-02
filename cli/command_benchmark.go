package cli

type commandBenchmark struct {
	compression commandBenchmarkCompression
	crypto      commandBenchmarkCrypto
	splitters   commandBenchmarkSplitters
}

func (c *commandBenchmark) setup(parent commandParent) {
	cmd := parent.Command("benchmark", "Commands to test performance of algorithms.").Hidden()

	c.compression.setup(cmd)
	c.crypto.setup(cmd)
	c.splitters.setup(cmd)
}
