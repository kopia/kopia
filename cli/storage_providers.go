package cli

import (
	"context"

	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/throttling"
)

type storageProviderServices interface {
	setPasswordFromToken(pwd string)
	AddStorageProvider(p StorageProvider)
	storageProviders() []StorageProvider
}

// StorageFlags is implemented by cli storage providers which need to support a
// particular backend. This requires the common setup and connection methods
// implemented by all the cli storage providers.
type StorageFlags interface {
	setup(sps storageProviderServices, cmd *kingpin.CmdClause)
	connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error)
}

// StorageProvider is a CLI provider for storage options and allows the CLI to
// multiplex between various provider CLI flag constructors.
type StorageProvider struct {
	Name        string
	Description string
	NewFlags    func() StorageFlags
}

func commonThrottlingFlags(cmd *kingpin.CmdClause, limits *throttling.Limits) {
	cmd.Flag("max-download-speed", "Limit the download speed.").PlaceHolder("BYTES_PER_SEC").FloatVar(&limits.DownloadBytesPerSecond)
	cmd.Flag("max-upload-speed", "Limit the upload speed.").PlaceHolder("BYTES_PER_SEC").FloatVar(&limits.UploadBytesPerSecond)
}

// AddStorageProvider adds a new StorageProvider at runtime after the App has
// been initialized with the default providers. This is used in tests which
// require custom storage providers to simulate various edge cases.
func (c *App) AddStorageProvider(p StorageProvider) {
	c.cliStorageProviders = append(c.cliStorageProviders, p)
}

func (c *App) storageProviders() []StorageProvider {
	return c.cliStorageProviders
}
