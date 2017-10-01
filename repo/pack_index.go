package repo

import (
	"bufio"
	"encoding/json"
	"io"
	"time"
)

type packIndexesOld map[string]*packIndex

type packIndexes []*packIndex

type packIndex struct {
	PackBlockID string            `json:"packBlock,omitempty"`
	PackGroup   string            `json:"packGroup,omitempty"`
	CreateTime  time.Time         `json:"createTime"`
	Items       map[string]string `json:"items"`
}

func loadPackIndexes(r io.Reader) (packIndexes, error) {
	b := bufio.NewReader(r)
	peek, err := b.Peek(1)
	if err != nil {
		return nil, err
	}

	if peek[0] == '{' {
		var old packIndexesOld

		if err := json.NewDecoder(b).Decode(&old); err != nil {
			return nil, err
		}

		var pi packIndexes
		for _, v := range old {
			pi = append(pi, v)
		}
		return pi, nil
	}

	var pi packIndexes

	if err := json.NewDecoder(b).Decode(&pi); err != nil {
		return nil, err
	}

	return pi, nil
}
