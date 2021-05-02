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

func (c *commandRepository) setup(app appServices, parent commandParent) {
	cmd := parent.Command("repository", "Commands to manipulate repository.").Alias("repo")

	c.connect.setup(app, cmd)
	c.create.setup(app, cmd)
	c.disconnect.setup(app, cmd)
	c.repair.setup(app, cmd)
	c.setClient.setup(app, cmd)
	c.status.setup(app, cmd)
	c.syncTo.setup(app, cmd)
	c.upgrade.setup(app, cmd)
}
