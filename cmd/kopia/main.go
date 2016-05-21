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
	app = kingpin.New("kopia", "Kopia - Online Backup").Version("0.0.1").Author("http://kopia.github.io/")
)

func main() {
	log.SetFlags(0)
	kingpin.MustParse(app.Parse(os.Args[1:]))
	return
}
