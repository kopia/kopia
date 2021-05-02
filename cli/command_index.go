package cli

type commandIndex struct {
	inspect  commandIndexInspect
	list     commandIndexList
	optimize commandIndexOptimize
	recover  commandIndexRecover
}

func (c *commandIndex) setup(parent commandParent) {
	cmd := parent.Command("index", "Commands to manipulate content index.").Hidden()

	c.inspect.setup(cmd)
	c.list.setup(cmd)
	c.optimize.setup(cmd)
	c.recover.setup(cmd)
}
