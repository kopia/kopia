package fs

import (
	"fmt"
	"sort"
)

// Directory represents contents of a directory.
type Directory struct {
	Entries []*Entry
}

// FindEntryName returns the pointer to an Entry with a given name or nil if not found.
func (d Directory) FindEntryName(name string) *Entry {
	entries := d.Entries
	i := sort.Search(
		len(entries),
		func(i int) bool { return entries[i].Name >= name },
	)

	if i < len(entries) && entries[i].Name == name {
		return entries[i]
	}

	return nil
}

func (d Directory) String() string {
	s := ""
	for i, f := range d.Entries {
		s += fmt.Sprintf("entry[%v] = %v\n", i, f)
	}

	return s
}
