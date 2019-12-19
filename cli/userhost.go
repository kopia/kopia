package cli

import (
	"os"
	"os/user"
	"runtime"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	hostName = getDefaultHostName()
	userName = getDefaultUserName()
)

func getUserName() string {
	return userName
}

func getDefaultUserName() string {
	currentUser, err := user.Current()
	if err != nil {
		log.Warningf("Cannot determine current user: %s", err)
		return "nobody"
	}

	u := currentUser.Username

	if runtime.GOOS == "windows" {
		if p := strings.Index(u, "\\"); p >= 0 {
			// On Windows ignore domain name.
			u = u[p+1:]
		}
	}

	return u
}

func getHostName() string {
	return hostName
}

func getDefaultHostName() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Warningf("Unable to determine hostname: %s", err)
		return "nohost"
	}

	// Normalize hostname.
	hostname = strings.ToLower(strings.Split(hostname, ".")[0])

	return hostname
}

func addUserAndHostFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("hostname", "Override default hostname.").StringVar(&hostName)
	cmd.Flag("username", "Override default username.").StringVar(&userName)
}
