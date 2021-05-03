package cli

import (
	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/repo/content"
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
		return content.AllPrefixedIDs
	}

	if c.contentIDNonPrefixed {
		return content.AllNonPrefixedIDs
	}

	return content.PrefixRange(content.ID(c.contentIDPrefix))
}
