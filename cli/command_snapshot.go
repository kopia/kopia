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

func (c *commandSnapshot) setup(parent commandParent) {
	cmd := parent.Command("snapshot", "Commands to manipulate snapshots.").Alias("snap")
	c.copyHistory.setup(cmd, false)
	c.moveHistory.setup(cmd, true)
	c.create.setup(cmd)
	c.delete.setup(cmd)
	c.estimate.setup(cmd)
	c.expire.setup(cmd)
	c.gc.setup(cmd)
	c.list.setup(cmd)
	c.migrate.setup(cmd)
	c.restore.setup(cmd)
	c.verify.setup(cmd)
}
