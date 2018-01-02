package block

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/kopia/kopia/internal/blockmgrpb"
)

type packIndexes []*packIndex

type offsetAndSize struct {
	offset uint32
	size   uint32
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

	off, err := strconv.ParseUint(s[0:p], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid format %q, can't parse offset: %v", s, err)
	}

	o.offset = uint32(off)

	siz, err := strconv.ParseUint(s[p+1:], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid format %q, can't parse offset: %v", s, err)
	}

	o.size = uint32(siz)
	return nil
}

type packIndex struct {
	PackBlockID  string                   `json:"packBlock,omitempty"`
	PackGroup    string                   `json:"packGroup,omitempty"`
	CreateTime   time.Time                `json:"createTime"`
	Items        map[string]offsetAndSize `json:"items"`
	DeletedItems []string                 `json:"deletedItems,omitempty"`
}

func loadPackIndexesLegacy(r io.Reader) ([]*blockmgrpb.Index, error) {
	var pi packIndexes

	if err := json.NewDecoder(r).Decode(&pi); err != nil {
		return nil, err
	}

	var result []*blockmgrpb.Index

	for _, v := range pi {
		result = append(result, convertLegacyIndex(v))
	}

	return result, nil
}

func convertLegacyIndex(pi *packIndex) *blockmgrpb.Index {
	res := &blockmgrpb.Index{
		CreateTimeNanos: pi.CreateTime.UnixNano(),
		DeletedItems:    pi.DeletedItems,
		PackBlockId:     pi.PackBlockID,
		PackGroup:       pi.PackGroup,
	}

	if len(pi.Items) > 0 {
		res.Items = make(map[string]uint64)

		for k, v := range pi.Items {
			res.Items[k] = packOffsetAndSize(v.offset, v.size)
		}
	}

	return res
}

func loadPackIndexesNew(data []byte) ([]*blockmgrpb.Index, error) {
	var b blockmgrpb.Indexes

	if err := proto.Unmarshal(data, &b); err != nil {
		return nil, err
	}

	return b.Indexes, nil
}

func loadPackIndexes(data []byte) ([]*blockmgrpb.Index, error) {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return loadPackIndexesNew(data)
	}

	return loadPackIndexesLegacy(gz)
}
