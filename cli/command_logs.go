package cli

type commandLogs struct {
	list    commandLogsList
	cleanup commandLogsCleanup
	show    commandLogsShow
}

func (c *commandLogs) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("logs", "Commands to manipulate logs stored in the repository.").Hidden().Alias("log")

	c.cleanup.setup(svc, cmd)
	c.list.setup(svc, cmd)
	c.show.setup(svc, cmd)
}
