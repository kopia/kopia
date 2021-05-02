package cli

type commandBlob struct {
	delete commandBlobDelete
	gc     commandBlobGC
	list   commandBlobList
	show   commandBlobShow
	stats  commandBlobStats
}

func (c *commandBlob) setup(parent commandParent) {
	cmd := parent.Command("blob", "Commands to manipulate BLOBs.").Hidden()

	c.delete.setup(cmd)
	c.gc.setup(cmd)
	c.list.setup(cmd)
	c.show.setup(cmd)
	c.stats.setup(cmd)
}
