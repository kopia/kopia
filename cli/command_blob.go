package cli

type commandBlob struct {
	delete commandBlobDelete
	gc     commandBlobGC
	list   commandBlobList
	shards commandBlobShards
	show   commandBlobShow
	stats  commandBlobStats
}

func (c *commandBlob) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("blob", "Commands to manipulate BLOBs.").Hidden()

	c.delete.setup(svc, cmd)
	c.gc.setup(svc, cmd)
	c.list.setup(svc, cmd)
	c.shards.setup(svc, cmd)
	c.show.setup(svc, cmd)
	c.stats.setup(svc, cmd)
}
