package cli

type commandContent struct {
	delete  commandContentDelete
	list    commandContentList
	rewrite commandContentRewrite
	show    commandContentShow
	stats   commandContentStats
	verify  commandContentVerify
}

func (c *commandContent) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("content", "Commands to manipulate content in repository.").Alias("contents").Hidden()

	c.delete.setup(svc, cmd)
	c.list.setup(svc, cmd)
	c.rewrite.setup(svc, cmd)
	c.show.setup(svc, cmd)
	c.stats.setup(svc, cmd)
	c.verify.setup(svc, cmd)
}
