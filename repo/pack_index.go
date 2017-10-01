package repo

import (
	"encoding/json"
	"io"
	"time"
)

type packIndexes []*packIndex

type packIndex struct {
	PackBlockID string            `json:"packBlock,omitempty"`
	PackGroup   string            `json:"packGroup,omitempty"`
	CreateTime  time.Time         `json:"createTime"`
	Items       map[string]string `json:"items"`
}

func loadPackIndexes(r io.Reader) (packIndexes, error) {
	var pi packIndexes

	if err := json.NewDecoder(r).Decode(&pi); err != nil {
		return nil, err
	}

	return pi, nil
}
