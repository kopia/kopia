/*
The 'kopia' utility support screating and accessing backups from command line.

Usage:

  $ kopia [<flags>] <subcommand> [<args> ...]

Common subcommands:

  init <provider> [<args> ...]
    Connects to the backup cas.

  backup [<flags>] <directory>...
    Copies local directory to backup repository.

  mount --objectID=CHUNKID <mountpoint>
    Mounts remote backup as local directory.

Use 'kopia help' to see more details.
*/
package main

import (
	"log"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("kopia", "Kopia - Online Backup")
)

func main() {
	log.SetFlags(0)
	kingpin.MustParse(app.Parse(os.Args[1:]))
	return
}
