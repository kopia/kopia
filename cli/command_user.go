package cli

type commandServerUser struct {
	add    commandServerUserAddSet
	set    commandServerUserAddSet
	delete commandServerUserDelete
	hash   commandServerUserHashPassword
	info   commandServerUserInfo
	list   commandServerUserList
}

func (c *commandServerUser) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("users", "Manager repository users").Alias("user")

	c.add.setup(svc, cmd, true)
	c.set.setup(svc, cmd, false)
	c.delete.setup(svc, cmd)
	c.hash.setup(svc, cmd)
	c.info.setup(svc, cmd)
	c.list.setup(svc, cmd)
}
