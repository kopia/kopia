package cli

type commandSnapshot struct {
	copyHistory commandSnapshotCopyMoveHistory
	moveHistory commandSnapshotCopyMoveHistory
	create      commandSnapshotCreate
	delete      commandSnapshotDelete
	estimate    commandSnapshotEstimate
	expire      commandSnapshotExpire
	fix         commandSnapshotFix
	list        commandSnapshotList
	migrate     commandSnapshotMigrate
	pin         commandSnapshotPin
	restore     commandSnapshotRestore
	verify      commandSnapshotVerify
}

func (c *commandSnapshot) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("snapshot", "Commands to manipulate snapshots.").Alias("snap")
	c.copyHistory.setup(svc, cmd, false)
	c.moveHistory.setup(svc, cmd, true)
	c.create.setup(svc, cmd)
	c.delete.setup(svc, cmd)
	c.estimate.setup(svc, cmd)
	c.expire.setup(svc, cmd)
	c.fix.setup(svc, cmd)
	c.list.setup(svc, cmd)
	c.migrate.setup(svc, cmd)
	c.pin.setup(svc, cmd)
	c.restore.setup(svc, cmd)
	c.verify.setup(svc, cmd)
}
