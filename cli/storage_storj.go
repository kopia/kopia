package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/storj"
)

type storageStorjFlags struct {
	storjOptions    storj.Options
	rootCaPemBase64 string //nolint:unused // TODO: maybe remove (this would only be needed for satellites with self-signed SSL certs?)
	rootCaPemPath   string //nolint:unused // TODO: maybe remove (this would only be needed for satellites with self-signed SSL certs?)
}

func (c *storageStorjFlags) Setup(svc StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("bucket", "Name of the storj bucket").Required().Envar(svc.EnvName("STORJ_BUCKET")).StringVar(&c.storjOptions.BucketName)
	cmd.Flag("access-name", "Config name associated with access (permissions etc.)").Required().Envar(svc.EnvName("STORJ_ACCESS_NAME")).StringVar(&c.storjOptions.AccessName)
	cmd.Flag("key-or-grant", "Access grant or API key (the latter requires satellite address and passphrase)").Required().Envar(svc.EnvName("STORJ_ACCESS_GRANT")).StringVar(&c.storjOptions.KeyOrGrant)
	// TODO: probably remove API key based access (remove below two flags)
	cmd.Flag("satellite-address", "Satellite address (host:port)"). /*PreAction(c.preActionReqSatAddr).*/ StringVar(&c.storjOptions.SatelliteAddr)
	cmd.Flag("passphrase", "Storj encryption passphrase (only for API key access)"). /*PreAction(c.preActionReqPassphrase).*/ StringVar(&c.storjOptions.Passphrase)

	commonThrottlingFlags(cmd, &c.storjOptions.Limits)

	// TODO error handling
}

// pre-actions do not work this way, let's skip this validation for now
// it is validated anyway during connect

// func (c *storageStorjFlags) preActionReqSatAddr(_ *kingpin.ParseContext) error {
// 	if len(c.storjOptions.KeyOrGrant) < 279 {
// 		if c.storjOptions.SatelliteAddr == "" {
// 			return fmt.Errorf("Satellite address required when using API key access")
// 		}
// 	}
// 	return nil
// }
//
// func (c *storageStorjFlags) preActionReqPassphrase(_ *kingpin.ParseContext) error {
// 	if len(c.storjOptions.KeyOrGrant) < 279 {
// 		if c.storjOptions.Passphrase == "" {
// 			return fmt.Errorf("Passphrase required when using API key access")
// 		}
// 	}
// 	return nil
// }

// Connect connects to the storj storage backend.
func (c *storageStorjFlags) Connect(ctx context.Context, createBucket bool, _ /* formatVersion */ int) (blob.Storage, error) {
	return storj.New(ctx, &c.storjOptions, createBucket) //nolint:wrapcheck
}
