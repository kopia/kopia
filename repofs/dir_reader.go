package repofs

import (
	"bufio"
	"fmt"
	"io"

	"github.com/kopia/kopia/internal/jsonstream"
	"github.com/kopia/kopia/repo"
)

var directoryStreamType = "kopia:directory"

// readDirEntries reads all the dirEntry from the specified reader.
func readDirEntries(r io.Reader) ([]*dirEntry, error) {
	psr, err := jsonstream.NewReader(bufio.NewReader(r), directoryStreamType)
	if err != nil {
		return nil, err
	}
	var entries []*dirEntry
	for {
		e := &dirEntry{}
		err := psr.Read(e)
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		entries = append(entries, e)
	}

	return flattenBundles(entries)
}

func flattenBundles(source []*dirEntry) ([]*dirEntry, error) {
	var entries []*dirEntry
	var bundles [][]*dirEntry

	for _, e := range source {
		if e.Type == entryTypeBundle {
			bundle := e.BundledChildren
			e.BundledChildren = nil

			var currentOffset int64

			for _, child := range bundle {
				child.ObjectID = repo.SectionObjectID(currentOffset, child.FileSize, e.ObjectID)
				currentOffset += child.FileSize
			}

			if currentOffset != e.FileSize {
				return nil, fmt.Errorf("inconsistent size of '%v': %v (got %v)", e.Name, e.FileSize, currentOffset)
			}

			bundles = append(bundles, bundle)
		} else {
			entries = append(entries, e)
		}
	}

	if len(bundles) > 0 {
		if entries != nil {
			bundles = append(bundles, entries)
		}

		entries = mergeSortN(bundles)
	}

	return entries, nil
}

func mergeSort2(b1, b2 []*dirEntry) []*dirEntry {
	combinedLength := len(b1) + len(b2)
	result := make([]*dirEntry, 0, combinedLength)

	for len(b1) > 0 && len(b2) > 0 {
		if b1[0].Name < b2[0].Name {
			result = append(result, b1[0])
			b1 = b1[1:]
		} else {
			result = append(result, b2[0])
			b2 = b2[1:]
		}
	}

	result = append(result, b1...)
	result = append(result, b2...)

	return result
}

func mergeSortN(slices [][]*dirEntry) []*dirEntry {
	switch len(slices) {
	case 1:
		return slices[0]
	case 2:
		return mergeSort2(slices[0], slices[1])
	default:
		mid := len(slices) / 2
		return mergeSort2(
			mergeSortN(slices[:mid]),
			mergeSortN(slices[mid:]))
	}
}
