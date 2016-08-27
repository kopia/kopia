package localfs

import (
	"os"

	"github.com/kopia/kopia/fs"
)

func populatePlatformSpecificEntryDetails(e *fs.EntryMetadata, fi os.FileInfo) error {
	return nil
}
