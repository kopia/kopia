package cli

type commandMaintenance struct {
	info commandMaintenanceInfo
	run  commandMaintenanceRun
	set  commandMaintenanceSet
}

func (c *commandMaintenance) setup(parent commandParent) {
	cmd := parent.Command("maintenance", "Maintenance commands.").Hidden().Alias("gc")

	c.info.setup(cmd)
	c.run.setup(cmd)
	c.set.setup(cmd)
}
