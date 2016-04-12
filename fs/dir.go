package fs

import "sort"

// Directory represents contents of a directory.
type Directory []*Entry

func (d Directory) Len() int      { return len(d) }
func (d Directory) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
func (d Directory) Less(i, j int) bool {
	return d[i].Name < d[j].Name
}

// FindByName returns Entry with a given name or nil if not found
func (d Directory) FindByName(n string) *Entry {
	i := sort.Search(
		len(d),
		func(i int) bool {
			return d[i].Name >= n
		},
	)
	if i < len(d) && d[i].Name == n {
		return d[i]
	}

	return nil
}
