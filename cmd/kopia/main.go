/*
The 'kopia' utility supports creating and accessing backups from command line.

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
	app          = kingpin.New("kopia", "Kopia - Online Backup").Author("http://kopia.github.io/")
	buildVersion = "UNKNOWN"
)

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stderr)
	app.Version(buildVersion)
	kingpin.MustParse(app.Parse(os.Args[1:]))
	return
}
