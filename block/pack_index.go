package block

import (
	"github.com/golang/protobuf/proto"
	"github.com/kopia/kopia/internal/blockmgrpb"
)

func loadPackIndexes(data []byte) ([]*blockmgrpb.Index, error) {
	var b blockmgrpb.Indexes

	if err := proto.Unmarshal(data, &b); err != nil {
		return nil, err
	}

	return b.Indexes, nil
}
