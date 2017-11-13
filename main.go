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
	"github.com/kopia/kopia/repo"

	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stderr)
	app := cli.App()
	app.Version(repo.BuildVersion + " build: " + repo.BuildInfo)
	kingpin.MustParse(app.Parse(os.Args[1:]))
	return
}
