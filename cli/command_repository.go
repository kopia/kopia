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

func (c *commandRepository) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("repository", "Commands to manipulate repository.").Alias("repo")

	c.connect.setup(svc, cmd)
	c.create.setup(svc, cmd)
	c.disconnect.setup(svc, cmd)
	c.repair.setup(svc, cmd)
	c.setClient.setup(svc, cmd)
	c.status.setup(svc, cmd)
	c.syncTo.setup(svc, cmd)
	c.upgrade.setup(svc, cmd)
}
