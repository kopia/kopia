package cli

type commandServerUser struct {
	add    commandServerUserAddSet
	set    commandServerUserAddSet
	delete commandServerUserDelete
	info   commandServerUserInfo
	list   commandServerUserList
}

func (c *commandServerUser) setup(app appServices, parent commandParent) {
	cmd := parent.Command("users", "Manager repository users").Alias("user")

	c.add.setup(app, cmd, true)
	c.set.setup(app, cmd, false)
	c.delete.setup(app, cmd)
	c.info.setup(app, cmd)
	c.list.setup(app, cmd)
}
