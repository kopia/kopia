package cli

import (
	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/repo/content"
)

var (
	contentIDPrefix      string
	contentIDNonPrefixed bool
	contentIDPrefixed    bool
)

func setupContentIDRangeFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("prefix", "Content ID prefix").StringVar(&contentIDPrefix)
	cmd.Flag("prefixed", "Apply to content IDs with (any) prefix").BoolVar(&contentIDPrefixed)
	cmd.Flag("non-prefixed", "Apply to content IDs without prefix").BoolVar(&contentIDNonPrefixed)
}

func contentIDRange() content.IDRange {
	if contentIDPrefixed {
		return content.AllPrefixedIDs
	}

	if contentIDNonPrefixed {
		return content.AllNonPrefixedIDs
	}

	return content.PrefixRange(content.ID(contentIDPrefix))
}
