package cli

type commandIndex struct {
	epoch commandIndexEpoch

	inspect  commandIndexInspect
	list     commandIndexList
	optimize commandIndexOptimize
	recover  commandIndexRecover
}

func (c *commandIndex) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("index", "Commands to manipulate content index.").Hidden()

	c.epoch.setup(svc, cmd)
	c.inspect.setup(svc, cmd)
	c.list.setup(svc, cmd)
	c.optimize.setup(svc, cmd)
	c.recover.setup(svc, cmd)
}
