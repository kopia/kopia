package cli

var snapshotRestoreCommand = snapshotCommands.Command("restore", restoreCommandHelp)

func init() {
	addRestoreFlags(snapshotRestoreCommand)
	snapshotRestoreCommand.Action(repositoryAction(runRestoreCommand))
}
