package cli

type commandBenchmark struct {
	compression commandBenchmarkCompression
	crypto      commandBenchmarkCrypto
	splitters   commandBenchmarkSplitters
}

func (c *commandBenchmark) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("benchmark", "Commands to test performance of algorithms.").Hidden()

	c.compression.setup(svc, cmd)
	c.crypto.setup(svc, cmd)
	c.splitters.setup(svc, cmd)
}
