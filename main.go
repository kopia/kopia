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

	"github.com/kopia/kopia/cli"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	buildVersion = "UNKNOWN"
)

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stderr)
	app := cli.App()
	app.Version(buildVersion)
	kingpin.MustParse(app.Parse(os.Args[1:]))
	return
}
