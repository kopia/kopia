// Package indextest provides utilities for testing content index.
package indextest

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/kopia/kopia/repo/content/index"
)

// InfoDiff returns a list of differences between two index.Info, empty if they are equal.
//
//nolint:gocyclo
func InfoDiff(i1, i2 index.Info, ignore ...string) []string {
	var diffs []string

	if l, r := i1.ContentID, i2.ContentID; l != r {
		diffs = append(diffs, fmt.Sprintf("GetContentID %v != %v", l, r))
	}

	if l, r := i1.PackBlobID, i2.PackBlobID; l != r {
		diffs = append(diffs, fmt.Sprintf("GetPackBlobID %v != %v", l, r))
	}

	if l, r := i1.Deleted, i2.Deleted; l != r {
		diffs = append(diffs, fmt.Sprintf("GetDeleted %v != %v", l, r))
	}

	if l, r := i1.FormatVersion, i2.FormatVersion; l != r {
		diffs = append(diffs, fmt.Sprintf("GetFormatVersion %v != %v", l, r))
	}

	if l, r := i1.OriginalLength, i2.OriginalLength; l != r {
		diffs = append(diffs, fmt.Sprintf("GetOriginalLength %v != %v", l, r))
	}

	if l, r := i1.PackOffset, i2.PackOffset; l != r {
		diffs = append(diffs, fmt.Sprintf("GetPackOffset %v != %v", l, r))
	}

	if l, r := i1.PackedLength, i2.PackedLength; l != r {
		diffs = append(diffs, fmt.Sprintf("GetPackedLength %v != %v", l, r))
	}

	if l, r := i1.TimestampSeconds, i2.TimestampSeconds; l != r {
		diffs = append(diffs, fmt.Sprintf("GetTimestampSeconds %v != %v", l, r))
	}

	if l, r := i1.Timestamp(), i2.Timestamp(); !l.Equal(r) {
		diffs = append(diffs, fmt.Sprintf("Timestamp %v != %v", l, r))
	}

	if l, r := i1.CompressionHeaderID, i2.CompressionHeaderID; l != r {
		diffs = append(diffs, fmt.Sprintf("GetCompressionHeaderID %v != %v", l, r))
	}

	if l, r := i1.EncryptionKeyID, i2.EncryptionKeyID; l != r {
		diffs = append(diffs, fmt.Sprintf("GetEncryptionKeyID %v != %v", l, r))
	}

	// dear future reader, if this fails because the number of methods has changed,
	// you need to add additional verification above.
	if cnt := reflect.TypeOf(index.Info{}).NumMethod(); cnt != 1 {
		diffs = append(diffs, fmt.Sprintf("unexpected number of methods on content.Info: %v, must update the test", cnt))
	}

	var result []string

	for _, v := range diffs {
		ignored := false

		for _, ign := range ignore {
			if strings.HasPrefix(v, ign) {
				ignored = true
			}
		}

		if !ignored {
			result = append(result, v)
		}
	}

	return result
}
