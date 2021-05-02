package cli

type commandContent struct {
	delete  commandContentDelete
	list    commandContentList
	rewrite commandContentRewrite
	show    commandContentShow
	stats   commandContentStats
	verify  commandContentVerify
}

func (c *commandContent) setup(parent commandParent) {
	cmd := parent.Command("content", "Commands to manipulate content in repository.").Alias("contents").Hidden()

	c.delete.setup(cmd)
	c.list.setup(cmd)
	c.rewrite.setup(cmd)
	c.show.setup(cmd)
	c.stats.setup(cmd)
	c.verify.setup(cmd)
}
