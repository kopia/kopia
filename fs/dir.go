package fs

import "sort"

// Directory represents contents of a directory.

type Directory []Entry

var emptyDirectory Directory

func (d Directory) FindByName(n string) Entry {
	i := sort.Search(len(d), func(i int) bool { return d[i].Name() >= n })
	if i < len(d) && d[i].Name() == n {
		return d[i]
	}

	return nil
}
