package fs

import "sort"

// Directory represents contents of a directory.
type Directory []*Entry

type sortedDirectory []*Entry

func (d sortedDirectory) Len() int      { return len(d) }
func (d sortedDirectory) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
func (d sortedDirectory) Less(i, j int) bool {
	if d[i].IsDir() != d[j].IsDir() {
		return d[i].IsDir()
	}

	return d[i].Name < d[j].Name
}

// FindByName returns Entry with a given name or nil if not found
func (d Directory) FindByName(isDir bool, n string) *Entry {
	i := sort.Search(
		len(d),
		func(i int) bool {
			if d[i].IsDir() != isDir {
				return !d[i].IsDir()
			}
			return d[i].Name >= n
		},
	)
	if i < len(d) && d[i].Name == n && d[i].IsDir() == isDir {
		return d[i]
	}

	return nil
}
