package repo

import (
	"bytes"
	"encoding/json"
	"io"
	"sort"
	"time"
)

const packIDPrefix = "K"

type packIndexes map[string]*packIndex

type packIndex struct {
	PackObject string            `json:"packObject"`
	PackGroup  string            `json:"packGroup,omitempty"`
	CreateTime time.Time         `json:"createTime"`
	Items      map[string]string `json:"items"`
}

func loadPackIndexes(r io.Reader) (packIndexes, error) {
	var pi packIndexes

	if err := json.NewDecoder(r).Decode(&pi); err != nil {
		return nil, err
	}

	return pi, nil
}

func (i packIndexes) merge(other packIndexes) {
	for packID, ndx := range other {
		i[packID] = ndx
	}
}

func loadMergedPackIndex(m map[string][]byte) (packIndexes, error) {
	var names []string
	for n := range m {
		names = append(names, n)
	}

	sort.Strings(names)

	merged := make(packIndexes)
	for _, n := range names {
		content := m[n]
		pi, err := loadPackIndexes(bytes.NewReader(content))
		if err != nil {
			return nil, err
		}
		merged.merge(pi)
	}

	return merged, nil
}
