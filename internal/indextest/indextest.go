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

	if l, r := i1.GetContentID(), i2.GetContentID(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetContentID %v != %v", l, r))
	}

	if l, r := i1.GetPackBlobID(), i2.GetPackBlobID(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetPackBlobID %v != %v", l, r))
	}

	if l, r := i1.GetDeleted(), i2.GetDeleted(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetDeleted %v != %v", l, r))
	}

	if l, r := i1.GetFormatVersion(), i2.GetFormatVersion(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetFormatVersion %v != %v", l, r))
	}

	if l, r := i1.GetOriginalLength(), i2.GetOriginalLength(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetOriginalLength %v != %v", l, r))
	}

	if l, r := i1.GetPackOffset(), i2.GetPackOffset(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetPackOffset %v != %v", l, r))
	}

	if l, r := i1.GetPackedLength(), i2.GetPackedLength(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetPackedLength %v != %v", l, r))
	}

	if l, r := i1.GetTimestampSeconds(), i2.GetTimestampSeconds(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetTimestampSeconds %v != %v", l, r))
	}

	if l, r := i1.Timestamp(), i2.Timestamp(); !l.Equal(r) {
		diffs = append(diffs, fmt.Sprintf("Timestamp %v != %v", l, r))
	}

	if l, r := i1.GetCompressionHeaderID(), i2.GetCompressionHeaderID(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetCompressionHeaderID %v != %v", l, r))
	}

	if l, r := i1.GetEncryptionKeyID(), i2.GetEncryptionKeyID(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetEncryptionKeyID %v != %v", l, r))
	}

	// dear future reader, if this fails because the number of methods has changed,
	// you need to add additional verification above.
	//nolint:gomnd
	if cnt := reflect.TypeOf((*index.InfoReader)(nil)).Elem().NumMethod(); cnt != 11 {
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
