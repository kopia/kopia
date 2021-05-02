package cli

type commandBlob struct {
	delete commandBlobDelete
	gc     commandBlobGC
	list   commandBlobList
	show   commandBlobShow
	stats  commandBlobStats
}

func (c *commandBlob) setup(app appServices, parent commandParent) {
	cmd := parent.Command("blob", "Commands to manipulate BLOBs.").Hidden()

	c.delete.setup(app, cmd)
	c.gc.setup(app, cmd)
	c.list.setup(app, cmd)
	c.show.setup(app, cmd)
	c.stats.setup(app, cmd)
}
