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
	appLogTimestamps *bool
	buildVersion     = "UNKNOWN"
)

func init() {
	appLogTimestamps = app.Flag("log-timestamps", "Log timestamps").Hidden().Action(enableLogTimestamps).Bool()
}

func enableLogTimestamps(context *kingpin.ParseContext) error {
	if *appLogTimestamps {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	} else {
		log.SetFlags(0)
	}
	return nil
}

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stderr)
	app.Version(buildVersion)
	kingpin.MustParse(app.Parse(os.Args[1:]))
	return
}
