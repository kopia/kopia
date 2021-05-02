package cli

type commandServerACL struct {
	add    commandACLAdd
	delete commandACLDelete
	enable commandACLEnable
	list   commandACLList
}

func (c *commandServerACL) setup(app appServices, parent commandParent) {
	cmd := parent.Command("acl", "Manager server access control list entries")

	c.add.setup(app, cmd)
	c.delete.setup(app, cmd)
	c.enable.setup(app, cmd)
	c.list.setup(app, cmd)
}
