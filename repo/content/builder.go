package content

import (
	"io"
	"sort"
)

// packIndexBuilder prepares and writes content index.
type packIndexBuilder map[ID]Info

// clone returns a deep clone of packIndexBuilder.
func (b packIndexBuilder) clone() packIndexBuilder {
	if b == nil {
		return nil
	}

	r := packIndexBuilder{}

	for k, v := range b {
		r[k] = v
	}

	return r
}

// Add adds a new entry to the builder or conditionally replaces it if the timestamp is greater.
func (b packIndexBuilder) Add(i Info) {
	old, ok := b[i.GetContentID()]
	if !ok || i.GetTimestampSeconds() >= old.GetTimestampSeconds() {
		b[i.GetContentID()] = i
	}
}

func (b packIndexBuilder) sortedContents() []Info {
	var allContents []Info

	for _, v := range b {
		allContents = append(allContents, v)
	}

	sort.Slice(allContents, func(i, j int) bool {
		return allContents[i].GetContentID() < allContents[j].GetContentID()
	})

	return allContents
}

// Build writes the pack index to the provided output.
func (b packIndexBuilder) Build(output io.Writer) error {
	return b.buildV1(output)
}
