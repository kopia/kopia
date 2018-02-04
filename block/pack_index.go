package block

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/kopia/kopia/internal/blockmgrpb"
)

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

func loadPackIndexes(data []byte) ([]*blockmgrpb.Index, error) {
	var b blockmgrpb.Indexes

	if err := proto.Unmarshal(data, &b); err != nil {
		return nil, err
	}

	return b.Indexes, nil
}
