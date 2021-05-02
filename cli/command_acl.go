package cli

type commandServerACL struct {
	add    commandACLAdd
	delete commandACLDelete
	enable commandACLEnable
	list   commandACLList
}

func (c *commandServerACL) setup(parent commandParent) {
	cmd := parent.Command("acl", "Manager server access control list entries")

	c.add.setup(cmd)
	c.delete.setup(cmd)
	c.enable.setup(cmd)
	c.list.setup(cmd)
}
