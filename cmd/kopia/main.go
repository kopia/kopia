/*
Command-line tool for creating and accessing backups.

Usage:

  $ kopia [<flags>] <subcommand> [<args> ...]

Use 'kopia help' to see more details.
*/
package main

import (
	"log"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app              = kingpin.New("kopia", "Kopia - Online Backup").Author("http://kopia.github.io/")
	appLogTimestamps = app.Flag("log-timestamps", "Log timestamps").Hidden().Action(enableLogTimestamps).Bool()
	buildVersion     = "UNKNOWN"
)

func enableLogTimestamps(context *kingpin.ParseContext) error {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	return nil
}

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stderr)
	app.Version(buildVersion)
	kingpin.MustParse(app.Parse(os.Args[1:]))
	return
}
