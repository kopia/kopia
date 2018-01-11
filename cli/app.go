package cli

import (
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("kopia", "Kopia - Online Backup").Author("http://kopia.github.io/")

	snapshotCommands   = app.Command("snapshot", "Commands to manipulate snapshots.").Alias("snap")
	policyCommands     = app.Command("policy", "Commands to manipulate snapshotting policies.").Alias("policies")
	metadataCommands   = app.Command("metadata", "Low-level commands to manipulate metadata items.").Alias("md")
	manifestCommands   = app.Command("manifest", "Low-level commands to manipulate manifest items.")
	objectCommands     = app.Command("object", "Commands to manipulate objects in repository.").Alias("obj")
	blockCommands      = app.Command("block", "Commands to manipulate blocks in repository.").Alias("blk")
	blockIndexCommands = blockCommands.Command("index", "Commands to manipulate block indexes.")
)

// App returns an instance of command-line application object.
func App() *kingpin.Application {
	return app
}
