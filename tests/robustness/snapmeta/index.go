//go:build darwin || (linux && amd64)

package snapmeta

// Index is a map of index name to the keys associated
// with that index name.
type Index map[string]map[string]struct{}

// AddToIndex adds a key to the index of the given name.
func (idx Index) AddToIndex(key, indexName string) {
	if _, ok := idx[indexName]; !ok {
		idx[indexName] = make(map[string]struct{})
	}

	idx[indexName][key] = struct{}{}
}

// RemoveFromIndex removes a key from the index of the given name.
func (idx Index) RemoveFromIndex(key, indexName string) {
	if _, ok := idx[indexName]; !ok {
		return
	}

	delete(idx[indexName], key)
}

// GetKeys returns the list of keys associated with the given index name.
func (idx Index) GetKeys(indexName string) (ret []string) {
	if _, ok := idx[indexName]; !ok {
		return ret
	}

	for k := range idx[indexName] {
		ret = append(ret, k)
	}

	return ret
}

// IsKeyInIndex will return true if the given index name contains the
// provided key.
func (idx Index) IsKeyInIndex(key, indexName string) bool {
	if _, ok := idx[indexName]; ok {
		_, keyExists := idx[indexName][key]
		return keyExists
	}

	return false
}
