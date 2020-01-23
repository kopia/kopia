package cli

import (
	"github.com/kopia/kopia/internal/serverapi"

	"github.com/pkg/errors"
)

var (
	serverAddress  = serverCommands.Flag("address", "Server address").Default("http://127.0.0.1:51515").String()
	serverUsername = serverCommands.Flag("server-username", "HTTP server username (basic auth)").Envar("KOPIA_SERVER_USERNAME").Default("kopia").String()
	serverPassword = serverCommands.Flag("server-password", "HTTP server password (basic auth)").Envar("KOPIA_SERVER_PASSWORD").String()
)

func serverAPIClientOptions() (serverapi.ClientOptions, error) {
	if *serverAddress == "" {
		return serverapi.ClientOptions{}, errors.Errorf("missing server address")
	}

	return serverapi.ClientOptions{
		BaseURL:  *serverAddress,
		Username: *serverUsername,
		Password: *serverPassword,
	}, nil
}
