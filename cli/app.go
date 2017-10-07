package cli

import (
	"log"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	app              = kingpin.New("kopia", "Kopia - Online Backup").Author("http://kopia.github.io/")
	appLogTimestamps *bool

	repositoryCommands = app.Command("repository", "Commands to manipulate repository.").Alias("repo")
	snapshotCommands   = app.Command("snapshot", "Commands to manipulate snapshots.").Alias("snap")
	policyCommands     = app.Command("policy", "Commands to manipulate snapshotting policies.").Alias("policies")
	metadataCommands   = app.Command("metadata", "Low-level commands to manipulate metadata items.").Alias("md")
	objectCommands     = app.Command("object", "Commands to manipulate objects in repository.").Alias("obj")
	blockCommands      = app.Command("block", "Commands to manipulate blocks in repository.").Alias("blk")
)

func init() {
	appLogTimestamps = app.Flag("log-timestamps", "Log timestamps").Hidden().Action(enableLogTimestamps).Bool()
}

// App returns an instance of command-line application object.
func App() *kingpin.Application {
	return app
}

func enableLogTimestamps(context *kingpin.ParseContext) error {
	if *appLogTimestamps {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	} else {
		log.SetFlags(0)
	}
	return nil
}
