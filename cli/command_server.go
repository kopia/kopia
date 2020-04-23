package cli

import (
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
)

var (
	serverAddress  = serverCommands.Flag("address", "Server address").Default("http://127.0.0.1:51515").String()
	serverUsername = serverCommands.Flag("server-username", "HTTP server username (basic auth)").Envar("KOPIA_SERVER_USERNAME").Default("kopia").String()
	serverPassword = serverCommands.Flag("server-password", "HTTP server password (basic auth)").Envar("KOPIA_SERVER_PASSWORD").String()
)

func serverAPIClientOptions() (apiclient.Options, error) {
	if *serverAddress == "" {
		return apiclient.Options{}, errors.Errorf("missing server address")
	}

	return apiclient.Options{
		BaseURL:  *serverAddress,
		Username: *serverUsername,
		Password: *serverPassword,
	}, nil
}
