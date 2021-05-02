package cli

type commandSnapshot struct {
	copyHistory commandSnapshotCopyMoveHistory
	moveHistory commandSnapshotCopyMoveHistory
	create      commandSnapshotCreate
	delete      commandSnapshotDelete
	estimate    commandSnapshotEstimate
	expire      commandSnapshotExpire
	gc          commandSnapshotGC
	list        commandSnapshotList
	migrate     commandSnapshotMigrate
	restore     commandSnapshotRestore
	verify      commandSnapshotVerify
}

func (c *commandSnapshot) setup(app appServices, parent commandParent) {
	cmd := parent.Command("snapshot", "Commands to manipulate snapshots.").Alias("snap")
	c.copyHistory.setup(app, cmd, false)
	c.moveHistory.setup(app, cmd, true)
	c.create.setup(app, cmd)
	c.delete.setup(app, cmd)
	c.estimate.setup(app, cmd)
	c.expire.setup(app, cmd)
	c.gc.setup(app, cmd)
	c.list.setup(app, cmd)
	c.migrate.setup(app, cmd)
	c.restore.setup(app, cmd)
	c.verify.setup(app, cmd)
}
