package cli

type commandMaintenance struct {
	info commandMaintenanceInfo
	run  commandMaintenanceRun
	set  commandMaintenanceSet
}

func (c *commandMaintenance) setup(app appServices, parent commandParent) {
	cmd := parent.Command("maintenance", "Maintenance commands.").Hidden().Alias("gc")

	c.info.setup(app, cmd)
	c.run.setup(app, cmd)
	c.set.setup(app, cmd)
}
