package cli

import (
	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/fs/cachefs"
)

var (
	maxCachedEntries     int
	maxCachedDirectories int
)

func setupFSCacheFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("max-cached-entries", "Limit the number of cached directory entries").Default("100000").IntVar(&maxCachedEntries)
	cmd.Flag("max-cached-dirs", "Limit the number of cached directories").Default("100").IntVar(&maxCachedDirectories)
}

func newFSCache() cachefs.DirectoryCacher {
	return cachefs.NewCache(&cachefs.Options{
		MaxCachedDirectories: maxCachedDirectories,
		MaxCachedEntries:     maxCachedEntries,
	})
}
