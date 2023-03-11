package cli

import (
	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/index"
)

type contentRangeFlags struct {
	contentIDPrefix      string
	contentIDNonPrefixed bool
	contentIDPrefixed    bool
}

func (c *contentRangeFlags) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("prefix", "Content ID prefix").StringVar(&c.contentIDPrefix)
	cmd.Flag("prefixed", "Apply to content IDs with (any) prefix").BoolVar(&c.contentIDPrefixed)
	cmd.Flag("non-prefixed", "Apply to content IDs without prefix").BoolVar(&c.contentIDNonPrefixed)
}

func (c *contentRangeFlags) contentIDRange() content.IDRange {
	if c.contentIDPrefixed {
		return index.AllPrefixedIDs
	}

	if c.contentIDNonPrefixed {
		return index.AllNonPrefixedIDs
	}

	return index.PrefixRange(content.IDPrefix(c.contentIDPrefix))
}
