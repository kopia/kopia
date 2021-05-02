package cli

type commandContent struct {
	delete  commandContentDelete
	list    commandContentList
	rewrite commandContentRewrite
	show    commandContentShow
	stats   commandContentStats
	verify  commandContentVerify
}

func (c *commandContent) setup(app appServices, parent commandParent) {
	cmd := parent.Command("content", "Commands to manipulate content in repository.").Alias("contents").Hidden()

	c.delete.setup(app, cmd)
	c.list.setup(app, cmd)
	c.rewrite.setup(app, cmd)
	c.show.setup(app, cmd)
	c.stats.setup(app, cmd)
	c.verify.setup(app, cmd)
}
