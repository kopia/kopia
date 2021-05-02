package cli

type commandServerUser struct {
	add    commandServerUserAddSet
	set    commandServerUserAddSet
	delete commandServerUserDelete
	info   commandServerUserInfo
	list   commandServerUserList
}

func (c *commandServerUser) setup(parent commandParent) {
	cmd := parent.Command("users", "Manager repository users").Alias("user")

	c.add.setup(cmd, true)
	c.set.setup(cmd, false)
	c.delete.setup(cmd)
	c.info.setup(cmd)
	c.list.setup(cmd)
}
