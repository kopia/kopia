package fs

import (
	"bufio"
	"fmt"
	"io"

	"github.com/kopia/kopia/internal"

	"github.com/kopia/kopia/repo"
)

type directoryWriter struct {
	w *internal.ProtoStreamWriter
}

func (dw *directoryWriter) WriteEntry(e *EntryMetadata, children []*EntryMetadata) error {
	return dw.w.Write(e)
}

func newDirectoryWriter(w io.WriteCloser) *directoryWriter {
	dw := &directoryWriter{
		w: internal.NewProtoStreamWriter(w, internal.ProtoStreamTypeDir),
	}

	return dw
}

func invalidDirectoryError(cause error) error {
	return fmt.Errorf("invalid directory data: %v", cause)
}

func readDirectoryMetadataEntries(r io.Reader) ([]*EntryMetadata, error) {
	psr := internal.NewProtoStreamReader(bufio.NewReader(r), internal.ProtoStreamTypeDir)
	var entries []*EntryMetadata
	for {
		e := &EntryMetadata{}
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

func flattenBundles(source []*EntryMetadata) ([]*EntryMetadata, error) {
	var entries []*EntryMetadata
	var bundles [][]*EntryMetadata

	for _, e := range source {
		if len(e.BundledChildren) > 0 {
			bundle := e.BundledChildren
			e.BundledChildren = nil

			var currentOffset int64

			for _, child := range bundle {
				id := repo.NewSectionObjectID(currentOffset, child.FileSize, e.ObjectId)
				child.ObjectId = &id
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

func mergeSort2(b1, b2 []*EntryMetadata) []*EntryMetadata {
	combinedLength := len(b1) + len(b2)
	result := make([]*EntryMetadata, 0, combinedLength)

	for len(b1) > 0 && len(b2) > 0 {
		if isLess(b1[0].Name, b2[0].Name) {
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

func mergeSortN(slices [][]*EntryMetadata) []*EntryMetadata {
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
