package cli

type commandRepository struct {
	connect          commandRepositoryConnect
	create           commandRepositoryCreate
	disconnect       commandRepositoryDisconnect
	repair           commandRepositoryRepair
	setClient        commandRepositorySetClient
	setParameters    commandRepositorySetParameters
	changePassword   commandRepositoryChangePassword
	status           commandRepositoryStatus
	syncTo           commandRepositorySyncTo
	validateProvider commandRepositoryValidateProvider
	upgrade          commandRepositoryUpgrade
}

func (c *commandRepository) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("repository", "Commands to manipulate repository.").Alias("repo")

	c.connect.setup(svc, cmd) // nolint:contextcheck
	c.create.setup(svc, cmd)  // nolint:contextcheck
	c.disconnect.setup(svc, cmd)
	c.repair.setup(svc, cmd) // nolint:contextcheck
	c.setClient.setup(svc, cmd)
	c.setParameters.setup(svc, cmd)
	c.status.setup(svc, cmd)
	c.syncTo.setup(svc, cmd) // nolint:contextcheck
	c.changePassword.setup(svc, cmd)
	c.validateProvider.setup(svc, cmd)
	c.upgrade.setup(svc, cmd)
}
