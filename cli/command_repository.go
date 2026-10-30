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
	throttle         commandRepositoryThrottle
	validateProvider commandRepositoryValidateProvider
	upgrade          commandRepositoryUpgrade
}

func (c *commandRepository) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("repository", "Commands to manipulate repository.").Alias("repo")

	c.connect.setup(svc, cmd)
	c.create.setup(svc, cmd)
	c.disconnect.setup(svc, cmd)
	c.repair.setup(svc, cmd)
	c.setClient.setup(svc, cmd)
	c.setParameters.setup(svc, cmd)
	c.status.setup(svc, cmd)
	c.syncTo.setup(svc, cmd)
	c.throttle.setup(svc, cmd)
	c.changePassword.setup(svc, cmd)
	c.validateProvider.setup(svc, cmd)
	c.upgrade.setup(svc, cmd)
}
