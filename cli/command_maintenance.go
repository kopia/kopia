package cli

type commandMaintenance struct {
	info commandMaintenanceInfo
	run  commandMaintenanceRun
	set  commandMaintenanceSet
}

func (c *commandMaintenance) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("maintenance", "Maintenance commands.").Hidden().Alias("gc")

	c.info.setup(svc, cmd)
	c.run.setup(svc, cmd)
	c.set.setup(svc, cmd)
}
