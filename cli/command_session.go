package cli

type commandSession struct {
	list commandSessionList
}

func (c *commandSession) setup(app appServices, parent commandParent) {
	cmd := parent.Command("session", "Session commands.").Hidden()

	c.list.setup(app, cmd)
}
