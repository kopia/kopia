package cli

type commandRepository struct {
	connect    commandRepositoryConnect
	create     commandRepositoryCreate
	disconnect commandRepositoryDisconnect
	repair     commandRepositoryRepair
	setClient  commandRepositorySetClient
	status     commandRepositoryStatus
	syncTo     commandRepositorySyncTo
	upgrade    commandRepositoryUpgrade
}

func (c *commandRepository) setup(parent commandParent) {
	cmd := parent.Command("repository", "Commands to manipulate repository.").Alias("repo")

	c.connect.setup(cmd)
	c.create.setup(cmd)
	c.disconnect.setup(cmd)
	c.repair.setup(cmd)
	c.setClient.setup(cmd)
	c.status.setup(cmd)
	c.syncTo.setup(cmd)
	c.upgrade.setup(cmd)
}
