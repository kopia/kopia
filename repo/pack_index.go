package repo

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

type packIndexes []*packIndex

type offsetAndSize struct {
	offset int32
	size   int32
}

func (o offsetAndSize) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("%v+%v", o.offset, o.size))
}

func (o *offsetAndSize) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	p := strings.IndexByte(s, '+')
	if p < 0 {
		return fmt.Errorf("invalid format %q, missing +", s)
	}

	off, err := strconv.ParseInt(s[0:p], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid format %q, can't parse offset: %v", s, err)
	}

	o.offset = int32(off)

	siz, err := strconv.ParseInt(s[p+1:], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid format %q, can't parse offset: %v", s, err)
	}

	o.size = int32(siz)
	return nil
}

type packIndex struct {
	PackBlockID string                   `json:"packBlock,omitempty"`
	PackGroup   string                   `json:"packGroup,omitempty"`
	CreateTime  time.Time                `json:"createTime"`
	Items       map[string]offsetAndSize `json:"items"`
}

func loadPackIndexes(r io.Reader) (packIndexes, error) {
	var pi packIndexes

	if err := json.NewDecoder(r).Decode(&pi); err != nil {
		return nil, err
	}

	return pi, nil
}
