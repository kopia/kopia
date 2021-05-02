package cli

type commandBenchmark struct {
	compression commandBenchmarkCompression
	crypto      commandBenchmarkCrypto
	splitters   commandBenchmarkSplitters
}

func (c *commandBenchmark) setup(app appServices, parent commandParent) {
	cmd := parent.Command("benchmark", "Commands to test performance of algorithms.").Hidden()

	c.compression.setup(app, cmd)
	c.crypto.setup(app, cmd)
	c.splitters.setup(app, cmd)
}
