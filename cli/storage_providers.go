package cli

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/storage"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

// RegisterStorageConnectFlags registers repository subcommand to connect to a storage
// or create new repository in a given storage.
func RegisterStorageConnectFlags(
	name string,
	description string,
	flags func(*kingpin.CmdClause),
	connect func(ctx context.Context) (storage.Storage, error)) {

	// Set up 'create' subcommand
	cc := createCommand.Command(name, "Create repository in "+description)
	flags(cc)
	cc.Action(func(ctx *kingpin.ParseContext) error {
		st, err := connect(getContext())
		if err != nil {
			return fmt.Errorf("can't connect to storage: %v", err)
		}

		return runCreateCommandWithStorage(st)
	})

	// Set up 'connect' subcommand
	cc = connectCommand.Command(name, "Connect to repository in "+description)
	flags(cc)
	cc.Action(func(ctx *kingpin.ParseContext) error {
		st, err := connect(getContext())
		if err != nil {
			return fmt.Errorf("can't connect to storage: %v", err)
		}

		return runConnectCommandWithStorage(st)
	})
}
