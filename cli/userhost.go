package cli

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	hostName = getDefaultHostName()
	userName = getDefaultUserName()
)

func addUserAndHostFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("hostname", "Override default hostname.").StringVar(&hostName)
	cmd.Flag("username", "Override default username.").StringVar(&userName)
}
