package cli

type commandIndex struct {
	inspect  commandIndexInspect
	list     commandIndexList
	optimize commandIndexOptimize
	recover  commandIndexRecover
}

func (c *commandIndex) setup(app appServices, parent commandParent) {
	cmd := parent.Command("index", "Commands to manipulate content index.").Hidden()

	c.inspect.setup(app, cmd)
	c.list.setup(app, cmd)
	c.optimize.setup(app, cmd)
	c.recover.setup(app, cmd)
}
