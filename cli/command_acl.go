package cli

type commandServerACL struct {
	add    commandACLAdd
	delete commandACLDelete
	enable commandACLEnable
	list   commandACLList
}

func (c *commandServerACL) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("acl", "Manager server access control list entries")

	c.add.setup(svc, cmd)
	c.delete.setup(svc, cmd)
	c.enable.setup(svc, cmd)
	c.list.setup(svc, cmd)
}
